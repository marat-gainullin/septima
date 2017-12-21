package net.sf.jsqlparser.test.select;

import junit.framework.TestCase;
import junit.textui.TestRunner;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.SeptimaSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.test.TestException;
import net.sf.jsqlparser.test.simpleparsing.SeptimaSqlParserTest;
import net.sf.jsqlparser.test.tablesfinder.TablesNamesFinder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PerformanceTest extends TestCase {

    private final static int NUM_REPS = 500;
    private SeptimaSqlParser parserManager = new SeptimaSqlParser();

    public PerformanceTest(String arg0) {
        super(arg0);
    }

    public void testSpeed() throws Exception {
        // all the statements in simple_parsing.txt
        URL simpleParsing = Thread.currentThread().getContextClassLoader().getResource("simple_parsing.txt");
        URL rubis = Thread.currentThread().getContextClassLoader().getResource("RUBiS-select-requests.txt");
        try (BufferedReader simpleParsingIn = new BufferedReader(new InputStreamReader(simpleParsing.openStream()));
                BufferedReader rubisIn = new BufferedReader(new InputStreamReader(rubis.openStream()))) {
            SeptimaSqlParserTest d;
            ArrayList statementsList = new ArrayList();

            while (true) {
                String statement = SeptimaSqlParserTest.getStatement(simpleParsingIn);
                if (statement == null) {
                    break;
                }
                statementsList.add(statement);
            }

            // all the statements in RUBiS-select-requests.txt
            while (true) {
                String line = SeptimaSqlParserTest.getLine(rubisIn);
                if (line == null) {
                    break;
                }
                if (line.length() == 0) {
                    continue;
                }

                if (!line.equals("#begin")) {
                    break;
                }
                line = SeptimaSqlParserTest.getLine(rubisIn);
                StringBuilder buf = new StringBuilder(line);
                while (true) {
                    line = SeptimaSqlParserTest.getLine(rubisIn);
                    if (line.equals("#end")) {
                        break;
                    }
                    buf.append("\n");
                    buf.append(line);
                }
                if (!SeptimaSqlParserTest.getLine(rubisIn).equals("true")) {
                    continue;
                }

                statementsList.add(buf.toString());

                String cols = SeptimaSqlParserTest.getLine(rubisIn);
                String tables = SeptimaSqlParserTest.getLine(rubisIn);
                String whereCols = SeptimaSqlParserTest.getLine(rubisIn);
                String type = SeptimaSqlParserTest.getLine(rubisIn);

            }

            String statement = "";
            int numTests = 0;
            // it seems that the very first parsing takes a while, so I put it aside
            Statement parsedStm = parserManager.parse(new StringReader(statement = (String) statementsList.get(0)));
            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
            ArrayList parsedSelects = new ArrayList(NUM_REPS * statementsList.size());
            long time = System.currentTimeMillis();

            // measure the time to parse NUM_REPS times all statements in the 2 files
            for (int i = 0; i < NUM_REPS; i++) {
                try {
                    int j = 0;
                    for (Iterator iter = statementsList.iterator(); iter.hasNext(); j++) {
                        statement = (String) iter.next();
                        parsedStm = parserManager.parse(new StringReader(statement));
                        numTests++;
                        if (parsedStm instanceof Select) {
                            parsedSelects.add(parsedStm);
                        }

                    }
                } catch (JSQLParserException e) {
                    throw new TestException("impossible to parse statement: " + statement, e);
                }
            }
            long elapsedTime = System.currentTimeMillis() - time;
            long statementsPerSecond = numTests * 1000 / elapsedTime;
            DecimalFormat df = new DecimalFormat();
            df.setMaximumFractionDigits(7);
            df.setMinimumFractionDigits(4);
            System.out.println(numTests + " statements parsed in " + elapsedTime + " milliseconds");
            System.out.println(" ("
                    + statementsPerSecond + " statements per second,  " + df.format(1.0 / statementsPerSecond) + " seconds per statement )");

            numTests = 0;
            time = System.currentTimeMillis();
            // measure the time to get the tables names from all the SELECTs parsed before
            for (Iterator iter = parsedSelects.iterator(); iter.hasNext();) {
                Select select = (Select) iter.next();
                if (select != null) {
                    numTests++;
                    List tableListRetr = tablesNamesFinder.getTableList(select);
                }
            }
            elapsedTime = System.currentTimeMillis() - time;
            statementsPerSecond = numTests * 1000 / elapsedTime;
            System.out.println(numTests + " select scans for table name executed in " + elapsedTime + " milliseconds");
            System.out.println(" ("
                    + statementsPerSecond
                    + " select scans for table name per second,  "
                    + df.format(1.0 / statementsPerSecond)
                    + " seconds per select scans for table name)");

        }
    }

    public static void main(String[] args) {
        TestRunner.run(PerformanceTest.class);
    }
}
