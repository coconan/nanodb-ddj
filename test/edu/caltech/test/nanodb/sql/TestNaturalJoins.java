package edu.caltech.test.nanodb.sql;


import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import org.testng.annotations.Test;


/**
 * This class tests the functionality of inner joins, left outer joins, and
 * right outer joins that use the NATURAL keyword. In particular, tests focus on
 * joins where one, none, and both tables are empty. For cases where neither
 * table is empty, there are rows that join with several in the other table.
 */
@Test
public class TestNaturalJoins extends SqlTestCase {

    public TestNaturalJoins() {
        super("setup_testJoins");
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
                new TupleLiteral(0, null, null),
                new TupleLiteral(0, null, 10),
                new TupleLiteral(0, null, 20),
                new TupleLiteral(0, 10, null),
                new TupleLiteral(0, 10, 10),
                new TupleLiteral(0, 10, 20),
                new TupleLiteral(2, 20, null),
                new TupleLiteral(3, 30, null),
                new TupleLiteral(4, null, null)
        };

        TupleLiteral[] expected3 = {
                new TupleLiteral(0, null, null),
                new TupleLiteral(0, 10, null),
                new TupleLiteral(2, 20, null),
                new TupleLiteral(3, 30, null),
                new TupleLiteral(4, null, null)
        };

        CommandResult result;

        // Two empty tables
        result = server.doCommand(
                "SELECT a, b, c FROM test_joins3 " +
                        "NATURAL LEFT OUTER JOIN test_joins4", true);
        assert checkUnorderedResults(expected1, result);

        // Right table is empty
        result = server.doCommand(
                "SELECT a, b, c FROM test_joins1 " +
                        "NATURAL LEFT OUTER JOIN test_joins4", true);
        assert checkUnorderedResults(expected3, result);

        // Left table is empty
        result = server.doCommand(
                "SELECT a, b, c FROM test_joins3 " +
                        "NATURAL LEFT OUTER JOIN test_joins2", true);
        assert checkUnorderedResults(expected1, result);

        // Neither table is empty
        result = server.doCommand(
                "SELECT a, b, c FROM test_joins1 " +
                        "NATURAL LEFT OUTER JOIN test_joins2", true);
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
                new TupleLiteral(0, null, null),
                new TupleLiteral(0, 10, null),
                new TupleLiteral(0, null, 10),
                new TupleLiteral(0, 10, 10),
                new TupleLiteral(0, null, 20),
                new TupleLiteral(0, 10, 20),
                new TupleLiteral(1, null, 30),
                new TupleLiteral(1, null, 40)
        };

        TupleLiteral[] expected3 = {
                new TupleLiteral(0, null, null),
                new TupleLiteral(0, null, 10),
                new TupleLiteral(0, null, 20),
                new TupleLiteral(1, null, 30),
                new TupleLiteral(1, null, 40)
        };

        CommandResult result;

        // Two empty tables
        result = server.doCommand(
                "SELECT a, b, c FROM test_joins3 " +
                        "NATURAL RIGHT OUTER JOIN test_joins4", true);
        assert checkUnorderedResults(expected1, result);

        // Right table is empty
        result = server.doCommand(
                "SELECT a, b, c FROM test_joins1 " +
                        "NATURAL RIGHT OUTER JOIN test_joins4", true);
        assert checkUnorderedResults(expected1, result);

        // Left table is empty
        result = server.doCommand(
                "SELECT a, b, c FROM test_joins3 " +
                        "NATURAL RIGHT OUTER JOIN test_joins2", true);
        assert checkUnorderedResults(expected3, result);

        // Neither table is empty
        result = server.doCommand(
                "SELECT a, b, c FROM test_joins1 " +
                        "NATURAL RIGHT OUTER JOIN test_joins2", true);
        assert checkUnorderedResults(expected2, result);
    }

    /**
     * This test performs some simple inner joins and checks the results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testInnerJoin() throws Throwable {
        TupleLiteral[] expected1 = {
        };

        TupleLiteral[] expected2 = {
                new TupleLiteral(0, null, null),
                new TupleLiteral(0, null, 10),
                new TupleLiteral(0, null, 20),
                new TupleLiteral(0, 10, null),
                new TupleLiteral(0, 10, 10),
                new TupleLiteral(0, 10, 20)
        };

        CommandResult result;

        // Two empty tables
        result = server.doCommand(
                "SELECT a, b, c FROM test_joins3 " +
                        "NATURAL JOIN test_joins4", true);
        assert checkUnorderedResults(expected1, result);

        // Right table is empty
        result = server.doCommand(
                "SELECT a, b, c FROM test_joins1 " +
                        "NATURAL JOIN test_joins4", true);
        assert checkUnorderedResults(expected1, result);

        // Left table is empty
        result = server.doCommand(
                "SELECT a, b, c FROM test_joins3 " +
                        "NATURAL JOIN test_joins2", true);
        assert checkUnorderedResults(expected1, result);

        // Neither table is empty
        result = server.doCommand(
                "SELECT a, b, c FROM test_joins1 " +
                        "NATURAL JOIN test_joins2", true);
        assert checkUnorderedResults(expected2, result);
    }
}
