package edu.caltech.nanodb.plans;


import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.relations.Tuple;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;


/**
 * PlanNode representing the <tt>LIMIT</tt> and <tt>OFFSET</tt>
 * clauses in a <tt>SELECT</tt>operation.
 */
public class LimitOffsetNode extends PlanNode {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(FileScanNode.class);

    /* The maximum number of tuples that are returned. */
    private int limit;

    /* The tuple index of the first tuple to return. */
    private int offset;

    /* The current index into the child tuple stream. */
    private int current;

    /* The marked position of current. */
    private int mark;

    /**
     * Constructs a LimitOffsetNode that limits the number of tuples returned and
     * can also start at any offset.
     */
    public LimitOffsetNode(PlanNode child, int limit, int offset) {
        super(OperationType.LIMITOFFSET, child);
        this.limit = limit;
        this.offset = offset;
    }


    /**
     * Creates a copy of this limit/offset node and its subtree.  This method is used
     * by {@link edu.caltech.nanodb.plans.PlanNode#duplicate} to copy a plan tree.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        LimitOffsetNode node = (LimitOffsetNode) super.clone();
        node.limit = limit;
        node.offset = offset;
        node.leftChild = leftChild.clone();
        return node;
    }

    /**
     * Ordering is the same as that of the child.
     */
    @Override
    public List<OrderByExpression> resultsOrderedBy() {
        return leftChild.resultsOrderedBy();
    }

    /**
     * Supports marking if the child does.
     */
    @Override
    public boolean supportsMarking() {
        return leftChild.supportsMarking();
    }

    /**
     * Requires marking if the child does.
     */
    @Override
    public boolean requiresLeftMarking() {
        return leftChild.requiresLeftMarking();
    }

    /**
     * No right child, so it can't require marking.
     */
    @Override
    public boolean requiresRightMarking() {
        return false;
    }

    /**
     * Gets the schema and stats of the child.
     */
    @Override
    public void prepare() {
        leftChild.prepare();
        schema = leftChild.getSchema();
        stats = leftChild.getStats();
    }

    /** Do initialization for the limit/offset operation. Resets state variables. */
    @Override
    public void initialize() {
        super.initialize();
        leftChild.initialize();
        current = 0;
        mark = 0;
    }


    /**
     * Moves up to the offset, and then gets the next tuple from the child
     * up to the limit.
     *
     * @return the tuple to be passed up to the next node.
     */
    public Tuple getNextTuple() throws IllegalStateException, IOException {
        logger.info(current);
        while (current - offset < 0) {
            current++;
            leftChild.getNextTuple();
        }
        if (current - offset < limit || limit == 0) {
            current++;
            return leftChild.getNextTuple();
        }
        return null;
    }

    /**
     * Store the current offset into the tuple stream.
     */
    @Override
    public void markCurrentPosition() {
        mark = current;
        leftChild.markCurrentPosition();
    }

    /**
     * Return to the stored offset.
     */
    @Override
    public void resetToLastMark() {
        current = mark;
        leftChild.resetToLastMark();
    }

    /**
     * Clean up the child.
     */
    @Override
    public void cleanUp() {
        leftChild.cleanUp();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "Limit/Offset: " + limit + "/" + offset + " ;" + leftChild.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LimitOffsetNode) {
            LimitOffsetNode othernode = (LimitOffsetNode) obj;
            return limit == othernode.limit && offset == othernode.offset && leftChild.equals(othernode.leftChild);
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return 17 * limit + 37 * offset + 7 * leftChild.hashCode();
    }

}
