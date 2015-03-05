package edu.caltech.nanodb.qeval;


import java.io.IOException;
import java.util.*;

import edu.caltech.nanodb.commands.SelectValue;
import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.plans.*;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Schema;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.FromClause;
import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This planner implementation uses dynamic programming to devise an optimal
 * join strategy for the query.  As always, queries are optimized in units of
 * <tt>SELECT</tt>-<tt>FROM</tt>-<tt>WHERE</tt> subqueries; optimizations
 * don't currently span multiple subqueries.
 */
public class CostBasedJoinPlanner implements Planner {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(CostBasedJoinPlanner.class);


    private StorageManager storageManager;


    public void setStorageManager(StorageManager storageManager) {
        this.storageManager = storageManager;
    }


    /**
     * This helper class is used to keep track of one "join component" in the
     * dynamic programming algorithm.  A join component is simply a query plan
     * for joining one or more leaves of the query.
     * <p>
     * In this context, a "leaf" may either be a base table or a subquery in
     * the <tt>FROM</tt>-clause of the query.  However, the planner will
     * attempt to push conjuncts down the plan as far as possible, so even if
     * a leaf is a base table, the plan may be a bit more complex than just a
     * single file-scan.
     */
    private static class JoinComponent {
        /**
         * This is the join plan itself, that joins together all leaves
         * specified in the {@link #leavesUsed} field.
         */
        public PlanNode joinPlan;

        /**
         * This field specifies the collection of leaf-plans that are joined by
         * the plan in this join-component.
         */
        public HashSet<PlanNode> leavesUsed;

        /**
         * This field specifies the collection of all conjuncts use by this join
         * plan.  It allows us to easily determine what join conjuncts still
         * remain to be incorporated into the query.
         */
        public HashSet<Expression> conjunctsUsed;

        /**
         * Constructs a new instance for a <em>leaf node</em>.  It should not
         * be used for join-plans that join together two or more leaves.  This
         * constructor simply adds the leaf-plan into the {@link #leavesUsed}
         * collection.
         *
         * @param leafPlan the query plan for this leaf of the query.
         *
         * @param conjunctsUsed the set of conjuncts used by the leaf plan.
         *        This may be an empty set if no conjuncts apply solely to
         *        this leaf, or it may be nonempty if some conjuncts apply
         *        solely to this leaf.
         */
        public JoinComponent(PlanNode leafPlan, HashSet<Expression> conjunctsUsed) {
            leavesUsed = new HashSet<PlanNode>();
            leavesUsed.add(leafPlan);

            joinPlan = leafPlan;

            this.conjunctsUsed = conjunctsUsed;
        }

        /**
         * Constructs a new instance for a <em>non-leaf node</em>.  It should
         * not be used for leaf plans!
         *
         * @param joinPlan the query plan that joins together all leaves
         *        specified in the <tt>leavesUsed</tt> argument.
         *
         * @param leavesUsed the set of two or more leaf plans that are joined
         *        together by the join plan.
         *
         * @param conjunctsUsed the set of conjuncts used by the join plan.
         *        Obviously, it is expected that all conjuncts specified here
         *        can actually be evaluated against the join plan.
         */
        public JoinComponent(PlanNode joinPlan, HashSet<PlanNode> leavesUsed,
                             HashSet<Expression> conjunctsUsed) {
            this.joinPlan = joinPlan;
            this.leavesUsed = leavesUsed;
            this.conjunctsUsed = conjunctsUsed;
        }
    }


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws java.io.IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    public PlanNode makePlan(SelectClause selClause,
        List<SelectClause> enclosingSelects) throws IOException {

        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Not yet implemented:  enclosing queries!");
        }
        // Get each part of the query.
        FromClause fromClause = selClause.getFromClause();
        Expression wherePredicate = selClause.getWhereExpr();
        Expression havingPredicate = selClause.getHavingExpr();
        List<Expression> groupByValues = selClause.getGroupByExprs();
        List<SelectValue> selectValues = selClause.getSelectValues();
        List<OrderByExpression> orderByValues = selClause.getOrderByExprs();

        // Convert the query into a plan tree using the join plan optimizer
        // Pass in predicates in where and having clauses
        HashSet<Expression> extras = new HashSet<Expression>();
        PredicateUtils.collectConjuncts(wherePredicate, extras);
        PlanNode plan = null;
        if (fromClause != null) {
            JoinComponent result = makeJoinPlan(fromClause, extras);
            plan = result.joinPlan;
        }

        // Grouping and aggregation.
        AggregateProcessor processor = new AggregateProcessor();
        // Check the FROM clause to see if there are any aggregate functions,
        // which is not valid SQL. To do this we must traverse the FromClause
        // tree.
        Queue<FromClause> nodesToVisit = new LinkedList<FromClause>();
        if (fromClause != null) {
            nodesToVisit.add(fromClause);
        }
        while (nodesToVisit.size() > 0) {
            FromClause node = nodesToVisit.poll();
            if (node.isJoinExpr()) {
                // Add child nodes.
                FromClause leftChild = node.getLeftChild();
                FromClause rightChild = node.getRightChild();
                assert leftChild != null && rightChild != null;
                nodesToVisit.add(leftChild);
                nodesToVisit.add(rightChild);

                // If this node is a JOIN .. ON node, then scan it for
                // aggregate functions.
                if (node.getConditionType() == FromClause.JoinConditionType.JOIN_ON_EXPR) {
                    scanForAggregates(node.getOnExpression(), processor);
                    if (processor.getAggregates().size() > 0) {
                        throw new IllegalArgumentException("Aggregate " +
                                "functions were found in a JOIN ON clause.");
                    }
                }
            }
        }

        // Check the WHERE clause for aggregate functions.
        // If there are aggregates found, the SQL is invalid.
        scanForAggregates(wherePredicate, processor);
        if (processor.getAggregates().size() > 0) {
            throw new IllegalArgumentException("Aggregate functions were " +
                    "found in the WHERE clause.");
        }

        // Scan the SELECT and HAVING clauses. Replace any aggregate functions
        // with ColumnValue expressions.
        for (SelectValue sv : selectValues) {
            if (!sv.isExpression()) {
                continue;
            }
            Expression e = scanForAggregates(sv.getExpression(), processor);
            sv.setExpression(e);
        }
        havingPredicate = scanForAggregates(havingPredicate, processor);

        // Now make the grouping and aggregation node if either grouping or
        // aggregation is necessary.
        Map<String, FunctionCall> aggregates = processor.getAggregates();
        if (aggregates.size() > 0 || groupByValues.size() > 0) {
            plan = new HashedGroupAggregateNode(plan, groupByValues, aggregates);
        }

        // HAVING predicate
        if (havingPredicate != null) {
            plan = new SimpleFilterNode(plan, havingPredicate);
        }

        // SELECT values (generalized project)
        if (!selClause.isTrivialProject()) {
            plan = new ProjectNode(plan, selectValues);
        } else if (fromClause != null) {
            ArrayList<SelectValue> values = fromClause.getPreparedSelectValues();
            if (values != null) {
                plan = new ProjectNode(plan, values);
            }
        }

        // If there's an ORDER BY, add a sort node
        if (!orderByValues.isEmpty()) {
            plan = new SortNode(plan, orderByValues);
        }

        // If the limit and offset are nonzero, add a limit/offset node
        if (selClause.getLimit() != 0 || selClause.getOffset() != 0) {
            plan = new LimitOffsetNode(plan, selClause.getLimit(), selClause.getOffset());
        }
        // Prepare the plan and return it
        plan.prepare();

        return plan;
    }


    /**
     * Given the top-level {@code FromClause} for a SELECT-FROM-WHERE block,
     * this helper generates an optimal join plan for the {@code FromClause}.
     *
     * @param fromClause the top-level {@code FromClause} of a
     *        SELECT-FROM-WHERE block.
     * @param extraConjuncts any extra conjuncts (e.g. from the WHERE clause,
     *        or HAVING clause)
     * @return a {@code JoinComponent} object that represents the optimal plan
     *         corresponding to the FROM-clause
     * @throws IOException if an IO error occurs during planning.
     */
    private JoinComponent makeJoinPlan(FromClause fromClause,
        Collection<Expression> extraConjuncts) throws IOException {

        // These variables receive the leaf-clauses and join conjuncts found
        // from scanning the sub-clauses.  Initially, we put the extra conjuncts
        // into the collection of conjuncts.
        HashSet<Expression> conjuncts = new HashSet<Expression>();
        ArrayList<FromClause> leafFromClauses = new ArrayList<FromClause>();

        collectDetails(fromClause, conjuncts, leafFromClauses);

        logger.debug("Making join-plan for " + fromClause);
        logger.debug("    Collected conjuncts:  " + conjuncts);
        logger.debug("    Collected FROM-clauses:  " + leafFromClauses);
        logger.debug("    Extra conjuncts:  " + extraConjuncts);

        if (extraConjuncts != null)
            conjuncts.addAll(extraConjuncts);

        // Make a read-only set of the input conjuncts, to avoid bugs due to
        // unintended side-effects.
        Set<Expression> roConjuncts = Collections.unmodifiableSet(conjuncts);

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.

        logger.debug("Generating plans for all leaves");
        ArrayList<JoinComponent> leafComponents = generateLeafJoinComponents(
            leafFromClauses, roConjuncts);

        // Print out the results, for debugging purposes.
        if (logger.isDebugEnabled()) {
            for (JoinComponent leaf : leafComponents) {
                logger.debug("    Leaf plan:  " +
                    PlanNode.printNodeTreeToString(leaf.joinPlan, true));
            }
        }

        // Build up the full query-plan using a dynamic programming approach.
        JoinComponent optimalJoin =
            generateOptimalJoin(leafComponents, roConjuncts);

        PlanNode plan = optimalJoin.joinPlan;

        logger.info("Optimal join plan generated:\n" +
            PlanNode.printNodeTreeToString(plan, true));

        return optimalJoin;
    }


    /**
     * This helper method pulls the essential details for join optimization
     * out of a <tt>FROM</tt> clause.
     *
     * If the given FROM clause is a leaf clause, it is added to the list of
     * leaf clauses, otherwise its predicates are broken up into conjuncts and
     * added to the set of conjuncts, and the function is called recursively on
     * its children. A FROM clause is a leaf iff it is a base table, a
     * derived table i.e. subquery, or an outer join.
     *
     * More intelligent planning and optimizing would allow us to also
     * optimize outer joins, but this is not yet implemented.
     *
     * @param fromClause the from-clause to collect details from
     *
     * @param conjuncts the collection to add all conjuncts to
     *
     * @param leafFromClauses the collection to add all leaf from-clauses to
     */
    private void collectDetails(FromClause fromClause,
        HashSet<Expression> conjuncts, ArrayList<FromClause> leafFromClauses) {

        // If the fromClause is a base table, a subquery, or an outer join, consider it a leaf
        // and add it to the list of leaves
        if (fromClause.isBaseTable() || fromClause.isDerivedTable() ||
                (fromClause.isJoinExpr() && fromClause.isOuterJoin())) {
            leafFromClauses.add(fromClause);
            return;
        }

        // Otherwise, not a leaf, so add its conjuncts
        PredicateUtils.collectConjuncts(fromClause.getOnExpression(), conjuncts);
        PredicateUtils.collectConjuncts(fromClause.getPreparedJoinExpr(), conjuncts);

        // And collect the details of the children if applicable
        if (fromClause.getLeftChild() != null) {
            collectDetails(fromClause.getLeftChild(), conjuncts, leafFromClauses);
        }

        if (fromClause.getRightChild() != null) {
            collectDetails(fromClause.getRightChild(), conjuncts, leafFromClauses);
        }
        return;
    }


    /**
     * This helper method performs the first step of the dynamic programming
     * process to generate an optimal join plan, by generating a plan for every
     * leaf from-clause identified from analyzing the query.  Leaf plans are
     * usually very simple; they are built either from base-tables or
     * <tt>SELECT</tt> subqueries.  The most complex detail is that any
     * conjuncts in the query that can be evaluated solely against a particular
     * leaf plan-node will be associated with the plan node.  <em>This is a
     * heuristic</em> that usually produces good plans (and certainly will for
     * the current state of the database), but could easily interfere with
     * indexes or other plan optimizations.
     *
     * @param leafFromClauses the collection of from-clauses found in the query
     *
     * @param conjuncts the collection of conjuncts that can be applied at this
     *                  level
     *
     * @return a collection of {@link JoinComponent} object containing the plans
     *         and other details for each leaf from-clause
     *
     * @throws IOException if a particular database table couldn't be opened or
     *         schema loaded, for some reason
     */
    private ArrayList<JoinComponent> generateLeafJoinComponents(
        Collection<FromClause> leafFromClauses, Collection<Expression> conjuncts)
        throws IOException {

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.
        ArrayList<JoinComponent> leafComponents = new ArrayList<JoinComponent>();
        for (FromClause leafClause : leafFromClauses) {
            HashSet<Expression> leafConjuncts = new HashSet<Expression>();

            PlanNode leafPlan =
                makeLeafPlan(leafClause, conjuncts, leafConjuncts);

            JoinComponent leaf = new JoinComponent(leafPlan, leafConjuncts);
            leafComponents.add(leaf);
        }

        return leafComponents;
    }


    /**
     * Constructs a plan tree for evaluating the specified from-clause.
     * Generates an optimal plan for the given leaf node, and a set of all
     * conjuncts used in the query. If any conjuncts are actually applied,
     * they are added to the leafConjuncts set. Base tables use a simple
     * select, subqueries make a plan recursively, and outer joins optimize
     * the two children before creating a joining node. Outer joins can also
     * pass conjuncts along, but only when the result is equivalent. Finally,
     * once the plan node is made, all the conjuncts that can be applied to
     * its scheme are collected and added to the plan node.
     *
     * @param fromClause the select nodes that need to be joined.
     *
     * @param conjuncts additional conjuncts that can be applied when
     *        constructing the from-clause plan.
     *
     * @param leafConjuncts this is an output-parameter.  Any conjuncts applied
     *        in this plan from the <tt>conjuncts</tt> collection should be added
     *        to this out-param.
     *
     * @return a plan tree for evaluating the specified from-clause
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     *
     * @throws IllegalArgumentException if the specified from-clause is a join
     *         expression that isn't an outer join, or has some other
     *         unrecognized type.
     */
    private PlanNode makeLeafPlan(FromClause fromClause,
        Collection<Expression> conjuncts, HashSet<Expression> leafConjuncts)
        throws IOException {

        // If no from clause exists, return null.
        if (fromClause == null) {
            return null;
        }

        PlanNode fromNode;
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
                if (fromClause.isRenamed()) {
                    fromNode = new RenameNode(fromNode, fromClause.getResultName());
                }
                break;
            case JOIN_EXPR:
                if (!fromClause.isOuterJoin()) { // Sanity check, must be an outer join
                    throw new IllegalArgumentException("Non Outer Join Leaf Node ????");
                }

                Collection<Expression> leftConjuncts = null;
                Collection<Expression> rightConjuncts = null;

                // Conjuncts can only be pushed into outer joins when the outer side is
                // not opposite the child the conjunct applies to
                if (!fromClause.hasOuterJoinOnLeft()) rightConjuncts = conjuncts;
                if (!fromClause.hasOuterJoinOnRight()) leftConjuncts = conjuncts;

                // For joins, evaluate left and right children, then generate the
                // join node.
                JoinComponent leftComponent = makeJoinPlan(fromClause.getLeftChild(), leftConjuncts);
                JoinComponent rightComponent = makeJoinPlan(fromClause.getRightChild(), rightConjuncts);

                // Add the conjuncts used, if any
                leafConjuncts.addAll(leftComponent.conjunctsUsed);
                leafConjuncts.addAll(rightComponent.conjunctsUsed);
                PlanNode left = leftComponent.joinPlan;
                PlanNode right = rightComponent.joinPlan;

                Expression predicate=  fromClause.getPreparedJoinExpr();
                boolean needProject = false;
                List<SelectValue> projectValues = null;

                JoinType joinType = fromClause.getJoinType();
                // Right outer joins can't be done with nested loop, so
                // convert to project + left join
                if (joinType == JoinType.RIGHT_OUTER) {
                    needProject = true;
                    // Get project values from the schema
                    projectValues = new ArrayList<SelectValue>();
                    Schema schema = fromClause.getPreparedSchema();
                    for (ColumnInfo colInfo : schema) {
                        SelectValue selVal = new SelectValue(
                                new ColumnValue(colInfo.getColumnName()), null);
                        projectValues.add(selVal);
                    }

                    // Switch right and left, then convert to left join
                    PlanNode temp = left;
                    left = right;
                    right = temp;
                    joinType = JoinType.LEFT_OUTER;
                }

                // Generate the join node
                fromNode = new NestedLoopsJoinNode(left, right, joinType, predicate);
                // Project if necessary
                if (needProject) {
                    fromNode = new ProjectNode(fromNode, projectValues);
                }
                break;

            default:
                throw new IllegalArgumentException("Illegal leaf node type " + fromClause.getClauseType().toString());
        }

        fromNode.prepare();

        // Optimize by adding predicates as soon as possible.
        HashSet<Expression> conjunctsToUse = new HashSet<Expression>();
        PredicateUtils.findExprsUsingSchemas(conjuncts, false, conjunctsToUse, fromNode.getSchema());
        if (!conjunctsToUse.isEmpty()) {
            leafConjuncts.addAll(conjunctsToUse);
            Expression predicate = PredicateUtils.makePredicate(conjunctsToUse);
            PlanUtils.addPredicateToPlan(fromNode, predicate);
            // Prepare the node again since it was changed
            fromNode.prepare();
        }

        return fromNode;
    }


    /**
     * This helper method builds up a full join-plan using a dynamic programming
     * approach.  The implementation maintains a collection of optimal
     * intermediate plans that join <em>n</em> of the leaf nodes, each with its
     * own associated cost, and then uses that collection to generate a new
     * collection of optimal intermediate plans that join <em>n+1</em> of the
     * leaf nodes.  This process completes when all leaf plans are joined
     * together; there will be <em>one</em> plan, and it will be the optimal
     * join plan (as far as our limited estimates can determine, anyway).
     *
     * @param leafComponents the collection of leaf join-components, generated
     *        by the {@link #generateLeafJoinComponents} method.
     *
     * @param conjuncts the collection of all conjuncts found in the query
     *
     * @return a single {@link JoinComponent} object that joins all leaf
     *         components together in an optimal way.
     */
    private JoinComponent generateOptimalJoin(
        ArrayList<JoinComponent> leafComponents, Set<Expression> conjuncts) {

        // This object maps a collection of leaf-plans (represented as a
        // hash-set) to the optimal join-plan for that collection of leaf plans.
        //
        // This collection starts out only containing the leaf plans themselves,
        // and on each iteration of the loop below, join-plans are grown by one
        // leaf.  For example:
        //   * In the first iteration, all plans joining 2 leaves are created.
        //   * In the second iteration, all plans joining 3 leaves are created.
        //   * etc.
        // At the end, the collection will contain ONE entry, which is the
        // optimal way to join all N leaves.  Go Go Gadget Dynamic Programming!
        HashMap<HashSet<PlanNode>, JoinComponent> joinPlans =
            new HashMap<HashSet<PlanNode>, JoinComponent>();

        // Initially populate joinPlans with just the N leaf plans.
        for (JoinComponent leaf : leafComponents)
            joinPlans.put(leaf.leavesUsed, leaf);

        while (joinPlans.size() > 1) {
            logger.debug("Current set of join-plans has " + joinPlans.size() +
                " plans in it.");

            // This is the set of "next plans" we will generate.  Plans only
            // get stored if they are the first plan that joins together the
            // specified leaves, or if they are better than the current plan.
            HashMap<HashSet<PlanNode>, JoinComponent> nextJoinPlans =
                new HashMap<HashSet<PlanNode>, JoinComponent>();

            // Iterate through each plan, then each leaf component
            for (Map.Entry<HashSet<PlanNode>, JoinComponent> entry :
                    joinPlans.entrySet()) {
                for (JoinComponent leaf : leafComponents) {
                    // If the current plan already contains the leaf, skip
                    // this iteration
                    if (entry.getKey().contains(leaf.joinPlan)) {
                        continue;
                    }

                    // Find unused conjuncts for the plan and the leaf
                    HashSet<Expression> unusedConjuncts =
                            new HashSet<Expression>(conjuncts);
                    HashSet<Expression> subplanConjuncts =
                            new HashSet<Expression>(leaf.conjunctsUsed);
                    subplanConjuncts.addAll(entry.getValue().conjunctsUsed);
                    unusedConjuncts.removeAll(subplanConjuncts);
                    HashSet<Expression> joinConjuncts = new HashSet<Expression>();

                    // Get schemas for new predicate
                    Schema leftSchema = entry.getValue().joinPlan.getSchema();
                    Schema rightSchema = leaf.joinPlan.getSchema();
                    // Use all unused conjuncts to generate the predicate
                    PredicateUtils.findExprsUsingSchemas(unusedConjuncts, true,
                            joinConjuncts, leftSchema, rightSchema);
                    Expression predicate = PredicateUtils.makePredicate(joinConjuncts);
                    // Add left/right child conjuncts to the used conjuncts set
                    joinConjuncts.addAll(leaf.conjunctsUsed);
                    joinConjuncts.addAll(entry.getValue().conjunctsUsed);

                    // Make join plan
                    NestedLoopsJoinNode nextPlan = new NestedLoopsJoinNode(
                            entry.getValue().joinPlan, leaf.joinPlan,
                            JoinType.INNER, predicate);

                    // Find cost for the plan
                    nextPlan.prepare();
                    PlanCost cost = nextPlan.getCost();
                    // Generate join component for the new plan
                    HashSet<PlanNode> leavesUsed =
                            new HashSet<PlanNode>(leaf.leavesUsed);
                    leavesUsed.addAll(entry.getKey());
                    JoinComponent component =
                            new JoinComponent(nextPlan, leavesUsed, joinConjuncts);

                    // Save plan if no other plan has a lower cost or no plan
                    // joining its leaf nodes exists yet.
                    if (nextJoinPlans.containsKey(leavesUsed)) {
                        float oldCost = nextJoinPlans.get(leavesUsed).
                                joinPlan.getCost().cpuCost;
                        if (cost.cpuCost < oldCost)
                            nextJoinPlans.put(leavesUsed, component);
                    }
                    else {
                        nextJoinPlans.put(leavesUsed, component);
                    }
                }
            }
            joinPlans = nextJoinPlans;
        }

        // At this point, the set of join plans should only contain one plan,
        // and it should be the optimal plan.

        assert joinPlans.size() == 1 : "There can be only one optimal join plan!";
        return joinPlans.values().iterator().next();
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

    /**
     * This helper is a wrapper for scanning expressions for aggregate functions.
     * The result is stored with the AggregateProcessor object.
     *
     * Returns the result node.
     */
    private Expression scanForAggregates(Expression e, AggregateProcessor p) {
        assert p != null;
        // If the expression is null, don't do anything.
        if (e == null) {
            return null;
        }
        return e.traverse(p);
    }
}
