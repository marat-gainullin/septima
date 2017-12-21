package net.sf.jsqlparser.test.simpleparsing;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.SeptimaSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.test.TestException;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URL;

public class SeptimaSqlParserTest {

    @Test
    public void testParse() throws Exception {
        SeptimaSqlParser parserManager = new SeptimaSqlParser();
        URL simpleParsing = Thread.currentThread().getContextClassLoader().getResource("simple_parsing.txt");
        try (BufferedReader in = new BufferedReader(new InputStreamReader(simpleParsing.openStream()))) {
            String statement = "";
            while (true) {
                try {
                    statement = SeptimaSqlParserTest.getStatement(in);
                    if (statement == null) {
                        break;
                    }

                    Statement parsedStm = parserManager.parse(new StringReader(statement));
                    //System.out.println(statement);
                } catch (JSQLParserException e) {
                    throw new TestException("impossible to parse statement: " + statement, e);
                }
            }
        }
    }

    public static String getStatement(BufferedReader in) throws Exception {
        StringBuffer buf = new StringBuffer();
        String line = null;
        while ((line = SeptimaSqlParserTest.getLine(in)) != null) {

            if (line.length() == 0) {
                break;
            }

            buf.append(line);
            buf.append("\n");

        }

        if (buf.length() > 0) {
            return buf.toString();
        } else {
            return null;
        }

    }

    public static String getLine(BufferedReader in) throws Exception {
        String line = null;
        while (true) {
            line = in.readLine();
            if (line != null) {
                line.trim();
//				if ((line.length() != 0) && ((line.length() < 2) ||  (line.length() >= 2) && !(line.charAt(0) == '/' && line.charAt(1) == '/')))
                if (((line.length() < 2) || (line.length() >= 2) && !(line.charAt(0) == '/' && line.charAt(1) == '/'))) {
                    break;
                }
            } else {
                break;
            }

        }

        return line;
    }
}
