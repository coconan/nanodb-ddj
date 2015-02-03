package edu.caltech.test.nanodb.sql;


import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import org.testng.annotations.Test;


/**
 * This class tests the functionality of cross joins, inner joins with a predicate,
 * left outer joins, and right outer joins.
 */
@Test
public class TestJoins extends SqlTestCase {

    public TestJoins() {
        super("setup_testJoins");
    }


    /**
     * This test performs some simple cross joins and checks the results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testCrossJoin() throws Throwable {

        TupleLiteral[] expected1 = {
        };

        TupleLiteral[] expected2 = {
                new TupleLiteral(0, 0, null, null),
                new TupleLiteral(0, 0, null, 10),
                new TupleLiteral(0, 0, null, 20),
                new TupleLiteral(0, 1, null, 30),
                new TupleLiteral(0, 1, null, 40),
                new TupleLiteral(0, 0, 10, null),
                new TupleLiteral(0, 0, 10, 10),
                new TupleLiteral(0, 0, 10, 20),
                new TupleLiteral(0, 1, 10, 30),
                new TupleLiteral(0, 1, 10, 40),
                new TupleLiteral(2, 0, 20, null),
                new TupleLiteral(2, 0, 20, 10),
                new TupleLiteral(2, 0, 20, 20),
                new TupleLiteral(2, 1, 20, 30),
                new TupleLiteral(2, 1, 20, 40),
                new TupleLiteral(3, 0, 30, null),
                new TupleLiteral(3, 0, 30, 10),
                new TupleLiteral(3, 0, 30, 20),
                new TupleLiteral(3, 1, 30, 30),
                new TupleLiteral(3, 1, 30, 40),
                new TupleLiteral(4, 0, null, null),
                new TupleLiteral(4, 0, null, 10),
                new TupleLiteral(4, 0, null, 20),
                new TupleLiteral(4, 1, null, 30),
                new TupleLiteral(4, 1, null, 40)
        };

        CommandResult result;

        // Two empty tables
        result = server.doCommand(
            "SELECT test_joins3.a, test_joins4.a, b, c FROM test_joins3 JOIN test_joins4", true);
        assert checkUnorderedResults(expected1, result);

        // Right table is empty
        result = server.doCommand(
                "SELECT test_joins1.a, test_joins4.a, b, c FROM test_joins1 JOIN test_joins4", true);
        assert checkUnorderedResults(expected1, result);

        // Left table is empty
        result = server.doCommand(
                "SELECT test_joins2.a, test_joins3.a, b, c FROM test_joins3 JOIN test_joins2", true);
        assert checkUnorderedResults(expected1, result);

        // Neither table is empty
        result = server.doCommand(
            "SELECT test_joins1.a, test_joins2.a, b, c FROM test_joins1 JOIN test_joins2", true);
        assert checkUnorderedResults(expected2, result);
    }


    /**
     * This test performs some simple left-outer joins and checks the results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testLeftOuterJoin() throws Throwable {

        TupleLiteral[] expected1 = {
        };

        TupleLiteral[] expected2 = {
                new TupleLiteral(0, 0, null, null),
                new TupleLiteral(0, 0, null, 10),
                new TupleLiteral(0, 0, null, 20),
                new TupleLiteral(0, 0, 10, null),
                new TupleLiteral(0, 0, 10, 10),
                new TupleLiteral(0, 0, 10, 20),
                new TupleLiteral(2, null, 20, null),
                new TupleLiteral(3, null, 30, null),
                new TupleLiteral(4, null, null, null)
        };

        TupleLiteral[] expected3 = {
                new TupleLiteral(0, null, null, null),
                new TupleLiteral(0, null, 10, null),
                new TupleLiteral(2, null, 20, null),
                new TupleLiteral(3, null, 30, null),
                new TupleLiteral(4, null, null, null)
        };

        CommandResult result;

        // Two empty tables
        result = server.doCommand(
                "SELECT test_joins3.a, test_joins4.a, b, c FROM test_joins3 " +
                        "LEFT OUTER JOIN test_joins4 ON test_joins3.a == test_joins4.a", true);
        assert checkUnorderedResults(expected1, result);

        // Right table is empty
        result = server.doCommand(
                "SELECT test_joins1.a, test_joins4.a, b, c FROM test_joins1 " +
                        "LEFT OUTER JOIN test_joins4 ON test_joins1.a == test_joins4.a", true);
        assert checkUnorderedResults(expected3, result);

        // Left table is empty
        result = server.doCommand(
                "SELECT test_joins2.a, test_joins3.a, b, c FROM test_joins3 " +
                        "LEFT OUTER JOIN test_joins2 ON test_joins2.a == test_joins3.a", true);
        assert checkUnorderedResults(expected1, result);

        // Neither table is empty
        result = server.doCommand(
                "SELECT test_joins1.a, test_joins2.a, b, c FROM test_joins1 " +
                        "LEFT OUTER JOIN test_joins2 ON test_joins1.a == test_joins2.a", true);
        assert checkUnorderedResults(expected2, result);
    }


    /**
     * This test performs some simple right-outer joins and checks the results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testRightOuterJoin() throws Throwable {
        TupleLiteral[] expected1 = {
        };

        TupleLiteral[] expected2 = {
                new TupleLiteral(0, 0, null, null),
                new TupleLiteral(0, 0, 10, null),
                new TupleLiteral(0, 0, null, 10),
                new TupleLiteral(0, 0, 10, 10),
                new TupleLiteral(0, 0, null, 20),
                new TupleLiteral(0, 0, 10, 20),
                new TupleLiteral(null, 1, null, 30),
                new TupleLiteral(null, 1, null, 40)
        };

        TupleLiteral[] expected3 = {
                new TupleLiteral(0, null, null, null),
                new TupleLiteral(0, null, null, 10),
                new TupleLiteral(0, null, null, 20),
                new TupleLiteral(1, null, null, 30),
                new TupleLiteral(1, null, null, 40)
        };

        CommandResult result;

        // Two empty tables
        result = server.doCommand(
                "SELECT test_joins3.a, test_joins4.a, b, c FROM test_joins3 " +
                        "RIGHT OUTER JOIN test_joins4 ON test_joins3.a == test_joins4.a", true);
        assert checkUnorderedResults(expected1, result);

        // Right table is empty
        result = server.doCommand(
                "SELECT test_joins1.a, test_joins4.a, b, c FROM test_joins1 " +
                        "RIGHT OUTER JOIN test_joins4 ON test_joins1.a == test_joins4.a", true);
        assert checkUnorderedResults(expected1, result);

        // Left table is empty
        result = server.doCommand(
                "SELECT test_joins2.a, test_joins3.a, b, c FROM test_joins3 " +
                        "RIGHT OUTER JOIN test_joins2 ON test_joins2.a == test_joins3.a", true);
        assert checkUnorderedResults(expected3, result);

        // Neither table is empty
        result = server.doCommand(
                "SELECT test_joins1.a, test_joins2.a, b, c FROM test_joins1 " +
                        "RIGHT OUTER JOIN test_joins2 ON test_joins1.a == test_joins2.a", true);
        assert checkUnorderedResults(expected2, result);
    }

    /**
     * This test performs some simple inner joins with a predicate and checks the results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testInnerJoin() throws Throwable {
        TupleLiteral[] expected1 = {
        };

        TupleLiteral[] expected2 = {
                new TupleLiteral(0, 0, null, null),
                new TupleLiteral(0, 0, null, 10),
                new TupleLiteral(0, 0, null, 20),
                new TupleLiteral(0, 0, 10, null),
                new TupleLiteral(0, 0, 10, 10),
                new TupleLiteral(0, 0, 10, 20)
        };

        CommandResult result;

        // Two empty tables
        result = server.doCommand(
                "SELECT test_joins3.a, test_joins4.a, b, c FROM test_joins3 " +
                        "JOIN test_joins4 ON test_joins3.a == test_joins4.a", true);
        assert checkUnorderedResults(expected1, result);

        // Right table is empty
        result = server.doCommand(
                "SELECT test_joins1.a, test_joins4.a, b, c FROM test_joins1 " +
                        "JOIN test_joins4 ON test_joins1.a == test_joins4.a", true);
        assert checkUnorderedResults(expected1, result);

        // Left table is empty
        result = server.doCommand(
                "SELECT test_joins2.a, test_joins3.a, b, c FROM test_joins3 " +
                        "JOIN test_joins2 ON test_joins2.a == test_joins3.a", true);
        assert checkUnorderedResults(expected1, result);

        // Neither table is empty
        result = server.doCommand(
                "SELECT test_joins1.a, test_joins2.a, b, c FROM test_joins1 " +
                        "JOIN test_joins2 ON test_joins1.a == test_joins2.a", true);
        assert checkUnorderedResults(expected2, result);
    }
}
