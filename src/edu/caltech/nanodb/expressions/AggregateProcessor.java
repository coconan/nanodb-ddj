package edu.caltech.nanodb.expressions;

import java.util.Map;
import java.util.HashMap;

import edu.caltech.nanodb.functions.Function;
import edu.caltech.nanodb.functions.AggregateFunction;

/**
 * Implementation of ExpressionProcessor to parse SELECT clause for
 * aggregate functions. You MUST use the same instance of this class for
 * parsing a given SELECT query, so that the renamed aggregate functions
 * remain consistent.
 */
public class AggregateProcessor implements ExpressionProcessor {

    private boolean foundAggregate;
    private HashMap<String, FunctionCall> aggregates;

    public AggregateProcessor() {
        this.foundAggregate = false;
        this.aggregates = new HashMap<String, FunctionCall>();
    }

    public Map<String, FunctionCall> getAggregates() {
        return this.aggregates;
    }

    /**
     * Enter a node.
     */
    public void enter(Expression node) {
        if (isAggregateFunction(node)) {
            if (this.foundAggregate) {
                throw new IllegalArgumentException("Aggregate functions cannot be nested within other aggregate functions!");
            }
            this.foundAggregate = true;
        }
    }

    /**
     * Leave a node.
     */
    public Expression leave(Expression node) {
        if (isAggregateFunction(node)) {
            assert node instanceof FunctionCall;
            // Clear the aggregate found flag (this should already be set
            // to true). If this is not already true, we have issues...
            assert this.foundAggregate;
            this.foundAggregate = false;

            // We want to replace this function with a
            // ColumnValue node with an auto-generated name.
            int aggregateCount = this.aggregates.size();
            String columnName = "#A" + aggregateCount;
            this.aggregates.put(columnName, (FunctionCall) node);
            return new ColumnValue(new ColumnName(columnName));
        }
        return node;
    }

    /**
     * Helper function to check if expression is an aggregate function.
     */
    private boolean isAggregateFunction(Expression node) {
        if (node instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) node;
            Function f = call.getFunction();
            if (f instanceof AggregateFunction) {
                return true;
            }
        }
        return false;
    }
}
