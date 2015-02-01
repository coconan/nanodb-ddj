package edu.caltech.nanodb.qeval;


import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.plans.*;
import edu.caltech.nanodb.relations.JoinType;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.FromClause;
import edu.caltech.nanodb.commands.SelectClause;

import edu.caltech.nanodb.expressions.Expression;

import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This class generates execution plans for performing SQL queries.  The
 * primary responsibility is to generate plans for SQL <tt>SELECT</tt>
 * statements, but <tt>UPDATE</tt> and <tt>DELETE</tt> expressions will also
 * use this class to generate simple plans to identify the tuples to update
 * or delete.
 */
public class SimplePlanner implements Planner {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SimplePlanner.class);


    private StorageManager storageManager;


    public void setStorageManager(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    /**
     * Returns the root of a plan tree representing a given FromClause.
     *
     * @param fromClause an object describing the relation to query
     *
     * @return a plan tree representing the given FromClause
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    private PlanNode fromClauseToNode(FromClause fromClause) throws IOException{
        PlanNode fromNode;
        // If no from clause exists, return null.
        if (fromClause == null){
            return null;
        }
        // Otherwise, switch based on from clause type
        switch (fromClause.getClauseType()) {
            // For base tables, file scan and rename if needed
            case BASE_TABLE:
                fromNode = makeSimpleSelect(fromClause.getTableName(),
                        null, null);
                if (fromClause.isRenamed()) {
                    fromNode = new RenameNode(fromNode,
                            fromClause.getResultName());
                }
                break;
            case SELECT_SUBQUERY:
                // For subqueries, recursively call makePlan and rename
                fromNode = makePlan(fromClause.getSelectClause(), null);
                fromNode.prepare();
                if (fromClause.isRenamed()) {
                    fromNode = new RenameNode(fromNode, fromClause.getResultName());
                }
                break;
            case JOIN_EXPR:
                // For joins, evaluate left and right children, then generate the
                // join node.
                PlanNode left = fromClauseToNode(fromClause.getLeftChild());
                PlanNode right = fromClauseToNode(fromClause.getRightChild());
                FromClause.JoinConditionType condition = fromClause.getConditionType();
                Expression predicate=  null;
                boolean needProject = false;

                // For ON clauses, predicate is the OnExpression
                if (condition == FromClause.JoinConditionType.JOIN_ON_EXPR) {
                    predicate = fromClause.getOnExpression();
                }
                // For other USING and NATURAL JOIN, predicate is
                // PreparedJoinExpr
                else if (condition == FromClause.JoinConditionType.JOIN_USING
                        || condition == FromClause.JoinConditionType.NATURAL_JOIN) {
                    // USING and NATURAL require a projected schema
                    needProject = true;
                    predicate = fromClause.getPreparedJoinExpr();
                }

                JoinType joinType = fromClause.getJoinType();
                // Right outer joins can't be done with nested loop, so
                // convert to project + left join
                if (joinType == JoinType.RIGHT_OUTER) {
                    needProject = true;
                    joinType = JoinType.LEFT_OUTER;
                }

                // Generate the join node
                fromNode = new NestedLoopsJoinNode(left, right, joinType, predicate);
                // Project if necessary
                if (needProject) {
                    fromNode = new ProjectNode(fromNode,
                            fromClause.getPreparedSelectValues());
                }
                break;
            default:
                // No other types should occur.
                logger.error("This should never occur");
                fromNode = null;
                break;
        }
        return fromNode;
    }
    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    @Override
    public PlanNode makePlan(SelectClause selClause,
        List<SelectClause> enclosingSelects) throws IOException {

        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Not yet implemented:  enclosing queries!");
        }

        // Get the from clause and convert it to a plan tree
        FromClause fromClause = selClause.getFromClause();
        PlanNode fromNode = fromClauseToNode(fromClause);
        PlanNode plan = fromNode;
        Expression predicate = selClause.getWhereExpr();
        // Insert a predicate if needed
        if (predicate != null) {
            plan = new SimpleFilterNode(plan, predicate);
        }

        // If there's a nontrivial project, add a project node.
        if (!selClause.isTrivialProject()) {
            plan = new ProjectNode(plan, selClause.getSelectValues());
        }

        // If there's an ORDER BY, add a sort node
        List<OrderByExpression> orderClause = selClause.getOrderByExprs();
        if (!orderClause.isEmpty()) {
            plan = new SortNode(plan, orderClause);
        }
        // Prepare the plan and return it
        plan.prepare();
        return plan;
    }


    /**
     * Constructs a simple select plan that reads directly from a table, with
     * an optional predicate for selecting rows.
     * <p>
     * While this method can be used for building up larger <tt>SELECT</tt>
     * queries, the returned plan is also suitable for use in <tt>UPDATE</tt>
     * and <tt>DELETE</tt> command evaluation.  In these cases, the plan must
     * only generate tuples of type {@link edu.caltech.nanodb.storage.PageTuple},
     * so that the command can modify or delete the actual tuple in the file's
     * page data.
     *
     * @param tableName The name of the table that is being selected from.
     *
     * @param predicate An optional selection predicate, or {@code null} if
     *        no filtering is desired.
     *
     * @return A new plan-node for evaluating the select operation.
     *
     * @throws IOException if an error occurs when loading necessary table
     *         information.
     */
    public SelectNode makeSimpleSelect(String tableName, Expression predicate,
        List<SelectClause> enclosingSelects) throws IOException {
        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        if (enclosingSelects != null) {
            // If there are enclosing selects, this subquery's predicate may
            // reference an outer query's value, but we don't detect that here.
            // Therefore we will probably fail with an unrecognized column
            // reference.
            logger.warn("Currently we are not clever enough to detect " +
                "correlated subqueries, so expect things are about to break...");
        }

        // Open the table.
        TableInfo tableInfo = storageManager.getTableManager().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        SelectNode selectNode = new FileScanNode(tableInfo, predicate);
        selectNode.prepare();
        return selectNode;
    }
}
