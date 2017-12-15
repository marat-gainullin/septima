/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.septima.http;

import com.septima.changes.*;
import com.septima.indexer.ScriptDocuments;
import com.septima.NamedValue;
import com.septima.client.scripts.ScriptedResource;
import com.septima.handlers.ChangesJSONReader;
import com.septima.script.Scripts;
import com.septima.util.RowsetJsonConstants;
import java.io.File;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Consumer;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author mg
 */
public class RequestReaderTest {

    protected static final String WRITTEN_CHANGES = "[{\"kind\":\"insert\", \"entity\":\"testEntity\", \"data\":{\"data\\\"\\\"1\":56, \"data2\":\"data2Value\", \"da\\\"ta3\":true, \"data4\":false, \"data5\":\"2012-08-27T11:42:15.514Z\"}},{\"kind\":\"update\", \"entity\":\"testEntity\", \"data\":{\"data\\\"\\\"1\":56, \"data2\":\"data2Value\", \"da\\\"ta3\":true, \"data4\":false, \"data5\":\"2012-08-27T11:42:15.514Z\"}, \"keys\":{\"key1\":78.9000015258789, \"key2\":\"key2Value\"}},{\"kind\":\"delete\", \"entity\":\"testEntity\", \"keys\":{\"key1\":78.9000015258789, \"key2\":\"key2Value\"}},{\"kind\":\"command\", \"entity\":\"testEntity\", \"parameters\":{\"key1\":78.9000015258789, \"key2\":\"key2Value\"}}]";

    @BeforeClass
    public static void init() throws Exception{
        Path platypusJsPath = ScriptedResource.lookupPlatypusJs();
        Scripts.init(platypusJsPath, false);
        Scripts.setOnlySpace(Scripts.createSpace());
        ScriptedResource.init(new Application(){
            @Override
            public Application.Type getType() {
                return Application.Type.CLIENT;
            }

            @Override
            public QueriesProxy getQueries() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public ModulesIndexer getModules() {
                return new ModulesIndexer(){
                    @Override
                    public ModuleStructure getModule(String string, Scripts.Space space, Consumer<ModuleStructure> cnsmr, Consumer<Exception> cnsmr1) throws Exception {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }

                    @Override
                    public File getResource(String string, Scripts.Space space, Consumer<File> cnsmr, Consumer<Exception> cnsmr1) throws Exception {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }

                    @Override
                    public Path getLocalPath() {
                        return platypusJsPath;
                    }

                    @Override
                    public File nameToFile(String string) throws Exception {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }

                    @Override
                    public String getDefaultModuleName(File file) {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }
                };
            }

            @Override
            public ServerModulesProxy getServerModules() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public ModelsDocuments getModels() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public FormsDocuments getForms() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public ReportsConfigs getReports() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public ScriptDocuments getScriptsConfigs() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        }, platypusJsPath, false);
        Scripts.getSpace().initSpaceGlobal();
    }
    
    @Test
    public void timeStampReadTest() throws ParseException {
        System.out.println("timeStampReadTest with millis");
        SimpleDateFormat sdf = new SimpleDateFormat(RowsetJsonConstants.DATE_FORMAT);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date dt = sdf.parse("2012-03-05T23:45:02.305Z");
        assertEquals(1330991102305L, dt.getTime());
    }

    @Test
    public void changesJsonReadTest() throws Exception {
        System.out.println("changesJsonReadTest");
        List<Change> changes = ChangesJSONReader.read(WRITTEN_CHANGES, Scripts.getSpace());

        NamedValue key1 = new NamedValue("key1", 78.9000015258789D);
        NamedValue key2 = new NamedValue("key2", "key2Value");
        NamedValue[] keys = new NamedValue[]{key1, key2};

        Date date = new Date(1346067735514L);
        NamedValue data1 = new NamedValue("data\"\"1", 56);
        NamedValue data2 = new NamedValue("data2", "data2Value");
        NamedValue data3 = new NamedValue("da\"ta3", true);
        NamedValue data4 = new NamedValue("data4", false);
        NamedValue data5 = new NamedValue("data5", date);
        NamedValue[] data = new NamedValue[]{data1, data2, data3, data4, data5};

        assertNotNull(changes);
        assertEquals(4, changes.size());
        assertTrue(changes.get(0) instanceof Insert);
        assertTrue(changes.get(1) instanceof Update);
        assertTrue(changes.get(2) instanceof Delete);
        assertTrue(changes.get(3) instanceof Command);
        Insert i = (Insert) changes.get(0);
        Update u = (Update) changes.get(1);
        Delete d = (Delete) changes.get(2);
        Command c = (Command) changes.get(3);
        assertEquals("testEntity", i.entityName);
        assertEquals("testEntity", u.entityName);
        assertEquals("testEntity", d.entityName);
        assertEquals("testEntity", c.entityName);
        assertNull(c.clause);
        assertNotNull(i.getData());
        assertEquals(5, i.getData().size());
        assertNotNull(u.getData());
        assertEquals(5, u.getData().size());
        for (int j = 0; j < i.getData().size(); j++) {
            assertNotSame(i.getData().get(j), u.getData().get(j));
            compareValues(i.getData().get(j), u.getData().get(j));
            compareValues(i.getData().get(j), data[j]);
        }
        assertNotNull(u.getKeys());
        assertEquals(2, u.getKeys().size());
        assertNotNull(d.getKeys());
        assertEquals(2, d.getKeys().size());
        assertNotNull(c.getParameters());
        assertEquals(2, c.getParameters().size());
        for (int j = 0; j < u.getKeys().size(); j++) {
            assertNotSame(u.getKeys().get(j), d.getKeys().get(j));
            compareValues(u.getKeys().get(j), d.getKeys().get(j));
            assertNotSame(u.getKeys().get(j), c.getParameters().get(j));
            compareValues(u.getKeys().get(j), c.getParameters().get(j));
            compareValues(u.getKeys().get(j), keys[j]);
        }
    }

    protected static void compareValues(NamedValue v1, NamedValue v2) {
        assertEquals(v1.name, v2.name);
        if(v1.value != null && !v1.value.equals(v2.value)){
            int h = 0;
            h++;
        }
        assertEquals(v1.value, v2.value);
    }
}
