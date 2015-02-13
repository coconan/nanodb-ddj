package edu.caltech.test.nanodb.sql;


import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import org.testng.annotations.Test;


/**
 * This class tests the functionality of the limit and offset clauses.
 */
@Test
public class TestLimitOffset extends SqlTestCase {

    public TestLimitOffset() {
        super("setup_limitoffset");
    }


    /**
     * This test attempts limiting the number of tuples, then attempts limiting
     * tuples at various offsets.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testCrossJoin() throws Throwable {

        TupleLiteral[] expected1 = {
                new TupleLiteral(0, "aaa"),
                new TupleLiteral(1, "bbb"),
                new TupleLiteral(2, "ccc"),
                new TupleLiteral(3, "ddd"),
                new TupleLiteral(4, "eee")
        };

        TupleLiteral[] expected2 = {
                new TupleLiteral(1, "bbb"),
                new TupleLiteral(2, "ccc"),
                new TupleLiteral(3, "ddd"),
                new TupleLiteral(4, "eee"),
                new TupleLiteral(5, "fff")
        };

        TupleLiteral[] expected3 = {
                new TupleLiteral(5, "fff"),
                new TupleLiteral(6, "ggg"),
                new TupleLiteral(7, "hhh"),
                new TupleLiteral(8, "iii"),
                new TupleLiteral(9, "jjj")
        };

        TupleLiteral[] expected4 = {
                new TupleLiteral(9, "jjj")
        };

        TupleLiteral[] expected5 = {
        };

        CommandResult result;

        result = server.doCommand(
            "SELECT * from test_limitoffset LIMIT 5;", true);
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
                "SELECT * from test_limitoffset LIMIT 5 OFFSET 1;", true);
        assert checkUnorderedResults(expected2, result);

        result = server.doCommand(
                "SELECT * from test_limitoffset LIMIT 5 OFFSET 5;", true);
        assert checkUnorderedResults(expected3, result);

        result = server.doCommand(
                "SELECT * from test_limitoffset LIMIT 5 OFFSET 9;", true);
        assert checkUnorderedResults(expected4, result);

        result = server.doCommand(
                "SELECT * from test_limitoffset LIMIT 5 OFFSET 10;", true);
        assert checkUnorderedResults(expected5, result);

    }
}
