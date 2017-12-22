package net.sf.jsqlparser.test.delete;

import java.io.StringReader;

import net.sf.jsqlparser.JSqlParserException;
import net.sf.jsqlparser.SeptimaSqlParser;
import net.sf.jsqlparser.statement.delete.Delete;
import static org.junit.Assert.*;
import org.junit.Test;

public class DeleteTest {

    SeptimaSqlParser parserManager = new SeptimaSqlParser();

    @Test
    public void testDelete() throws JSqlParserException {
        String statement = "DELETE FROM mytable WHERE mytable.col = 9";

        Delete delete = (Delete) parserManager.parse(new StringReader(statement));
        assertEquals("mytable", delete.getTable().getName());
        assertEquals(statement, "" + delete);
    }
    
    @Test
    public void testComment() throws JSqlParserException {
        String statement =
                "/*90053*/ DELETE /*werwer*/ FROM /*wefsdfjil*/ mytable /*90piop*/ WHERE mytable.col = 9 /*eiortouei*/";
        Delete delete = (Delete) parserManager.parse(new StringReader(statement));
        assertEquals(statement, "" + delete);
    }
}
