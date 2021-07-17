/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.sf.jsqlparser.test;

import net.sf.jsqlparser.JSqlParserException;
import net.sf.jsqlparser.SeptimaSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.StatementDeParser;
import org.junit.Before;
import org.junit.Test;

import java.io.StringReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 *
 * @author mg
 */
public class GeneralTest {

    protected SeptimaSqlParser parserManager;

    @Before
    public void setup() {
        parserManager = new SeptimaSqlParser();
    }

    @Test
    public void dummyTest()
    {
    }

    public Statement checkParseAndDeparse(String statementText) throws JSqlParserException {
        Statement statement = parserManager.parse(new StringReader(statementText));
        assertNotNull(statement);
        StringBuilder buffer = new StringBuilder();
        StatementDeParser deparser = new StatementDeParser(buffer);
        if (statement instanceof Select) {
            deparser.visit((Select) statement);
        } else if (statement instanceof Delete) {
            deparser.visit((Delete) statement);
        } else if (statement instanceof Insert) {
            deparser.visit((Insert) statement);
        } else {
            throw new JSqlParserException("Unknown type of parsed statement");
        }
        assertEquals(statementText.toLowerCase().replaceAll(ExpressionDeParser.LINE_SEPARATOR, ""), buffer.toString().toLowerCase().replaceAll(ExpressionDeParser.LINE_SEPARATOR, ""));
        return statement;
    }

}
