package edu.caltech.nanodb.plans;


import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.expressions.TupleLiteral;

import static edu.caltech.nanodb.relations.JoinType.ANTIJOIN;
import static edu.caltech.nanodb.relations.JoinType.LEFT_OUTER;


/**
 * This plan node implements a nested-loops join operation, which can support
 * arbitrary join conditions but is also the slowest join implementation.
 */
public class NestedLoopsJoinNode extends ThetaJoinNode {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SortMergeJoinNode.class);


    /** Most recently retrieved tuple of the left relation. */
    private Tuple leftTuple;

    /** Most recently retrieved tuple of the right relation. */
    private Tuple rightTuple;


    /** Set to true when we have exhausted all tuples from our subplans. */
    private boolean done;

    /** Set to true once a tuple on the left has matched a tuple on the right */
    private boolean matched;

    /** A cached tuple of nulls for padding **/
    private TupleLiteral rightNulls;


    public NestedLoopsJoinNode(PlanNode leftChild, PlanNode rightChild,
                JoinType joinType, Expression predicate) {

        super(leftChild, rightChild, joinType, predicate);
    }


    /**
     * Checks if the argument is a plan node tree with the same structure, but not
     * necessarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {

        if (obj instanceof NestedLoopsJoinNode) {
            NestedLoopsJoinNode other = (NestedLoopsJoinNode) obj;

            return predicate.equals(other.predicate) &&
                leftChild.equals(other.leftChild) &&
                rightChild.equals(other.rightChild);
        }

        return false;
    }


    /** Computes the hash-code of the nested-loops plan node. */
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (predicate != null ? predicate.hashCode() : 0);
        hash = 31 * hash + leftChild.hashCode();
        hash = 31 * hash + rightChild.hashCode();
        return hash;
    }


    /**
     * Returns a string representing this nested-loop join's vital information.
     *
     * @return a string representing this plan-node.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("NestedLoops[");

        if (predicate != null)
            buf.append("pred:  ").append(predicate);
        else
            buf.append("no pred");

        if (schemaSwapped)
            buf.append(" (schema swapped)");

        buf.append(']');

        return buf.toString();
    }


    /**
     * Creates a copy of this plan node and its subtrees.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        NestedLoopsJoinNode node = (NestedLoopsJoinNode) super.clone();

        // Clone the predicate.
        if (predicate != null)
            node.predicate = predicate.duplicate();
        else
            node.predicate = null;

        return node;
    }


    /**
     * Nested-loop joins can conceivably produce sorted results in situations
     * where the outer relation is ordered, but we will keep it simple and just
     * report that the results are not ordered.
     */
    @Override
    public List<OrderByExpression> resultsOrderedBy() {
        return null;
    }


    /** True if the node supports position marking. **/
    public boolean supportsMarking() {
        return leftChild.supportsMarking() && rightChild.supportsMarking();
    }


    /** True if the node requires that its left child supports marking. */
    public boolean requiresLeftMarking() {
        return false;
    }


    /** True if the node requires that its right child supports marking. */
    public boolean requiresRightMarking() {
        return false;
    }


    @Override
    public void prepare() {
        // Need to prepare the left and right child-nodes before we can do
        // our own work.
        leftChild.prepare();
        rightChild.prepare();

        // Use the parent class' helper-function to prepare the schema.
        prepareSchemaStats();

        if (joinType == LEFT_OUTER) rightNulls = new TupleLiteral(rightSchema.numColumns());

        // TODO:  Implement the rest
        cost = null;
    }


    public void initialize() {
        super.initialize();

        done = false;
        matched = false;
        leftTuple = null;
        rightTuple = null;
    }


    /**
     * Returns the next joined tuple that satisfies the join condition.
     *
     * @return the next joined tuple that satisfies the join condition.
     *
     * @throws IOException if a db file failed to open at some point
     */
    public Tuple getNextTuple() throws IOException {
        if (done)
            return null;

        while (getTuplesToJoin()) {
            switch (joinType) {
                case CROSS:
                    // Cross joins include all pairs of tuples
                    return joinTuples(leftTuple, rightTuple);
                case INNER:
                    // Inner joins include the pair if the predicate passes
                    if (canJoinTuples()) return joinTuples(leftTuple, rightTuple);
                    break;
                case LEFT_OUTER:
                    // Left outer joins can either include the pair, or include the left tuple
                    // padded with nulls on the right
                    if (rightTuple == null) {
                        matched = true;
                        return joinTuples(leftTuple, rightNulls);
                    } else if (canJoinTuples()) {
                        matched = true;
                        return joinTuples(leftTuple, rightTuple);
                    }
                    break;
                case SEMIJOIN:
                    // If the predicate is true, include the left tuple and break the loop by moving the left tuple
                    // forward and resetting the right child
                    if (canJoinTuples()) {
                        Tuple ret = leftTuple;
                        leftTuple = leftChild.getNextTuple();
                        rightChild.initialize();
                        rightTuple = rightChild.getNextTuple();
                        return ret;
                    }
                    break;
                case ANTIJOIN:
                    // If the right tuple returns null, the left tuple must have had no matches
                    if (rightTuple == null) {
                        matched = true;
                        return leftTuple;
                    } else if (canJoinTuples()) {
                        matched = true;
                    }
                    break;

                case RIGHT_OUTER:
                case FULL_OUTER:
                default:
                    // Right outer should have been converted to left outer in planning. Full outer is not supported
                    // by the nested loop algorithm.
                    throw new IllegalArgumentException("Unable to execute nested loop join of type " +
                                                                                String.valueOf(joinType));
            }
        }

        return null;
    }


    /**
     * This helper function implements the logic that sets {@link #leftTuple}
     * and {@link #rightTuple} based on the nested-loops logic.
     *
     * @return {@code true} if another pair of tuples was found to join, or
     *         {@code false} if no more pairs of tuples are available to join.
     */
    private boolean getTuplesToJoin() throws IOException {
        // This line initializes the very first tuple of the left node
        if (leftTuple == null && !done) leftTuple = leftChild.getNextTuple();

        if (leftTuple == null) {
            // The left node appears to have no tuples
            done = true;
            return false;
        }

        // Move the right tuple forward
        rightTuple = rightChild.getNextTuple();

        // If we have reached the end of the right child
        if (rightTuple == null) {
            // Return just the left tuple if this is a left outer or antijoin and it did not match with
            // any tuples on the right
            if (joinType == LEFT_OUTER && !matched) {
                return true;
            } else if (joinType == ANTIJOIN && !matched) {
                return true;
            }

            // Move the left tuple forward
            leftTuple = leftChild.getNextTuple();
            matched = false;

            // If there are no tuples left in the left node
            if (leftTuple == null) {
                done = true;
                return false;
            }

            // Reset the right child and move it forward
            rightChild.initialize();
            rightTuple = rightChild.getNextTuple();

            // If the right node has no tuples
            if (rightTuple == null) {
                // If this is a left outer join or antijoin we still want to return the left tuples. Otherwise
                // the right node is empty so just end here.
                return joinType == LEFT_OUTER || joinType == ANTIJOIN;
            }
        }
        return true;
    }


    private boolean canJoinTuples() {
        // If the predicate was not set, we can always join them!
        if (predicate == null)
            return true;

        environment.clear();
        environment.addTuple(leftSchema, leftTuple);
        environment.addTuple(rightSchema, rightTuple);

        return predicate.evaluatePredicate(environment);
    }


    public void markCurrentPosition() {
        leftChild.markCurrentPosition();
        rightChild.markCurrentPosition();
    }


    public void resetToLastMark() throws IllegalStateException {
        leftChild.resetToLastMark();
        rightChild.resetToLastMark();

        // TODO:  Prepare to reevaluate the join operation for the tuples.
        //        (Just haven't gotten around to implementing this.)
    }


    public void cleanUp() {
        leftChild.cleanUp();
        rightChild.cleanUp();
    }
}
