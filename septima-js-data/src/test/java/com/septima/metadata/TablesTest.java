package com.septima.metadata;

import com.septima.Database;
import com.septima.Metadata;
import com.septima.TestDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.naming.NamingException;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;

public class TablesTest {

    @BeforeClass
    public static void setup() throws NamingException {
        TestDataSource.bind();
    }

    @Test
    public void assetsCaseInsensitiveColumns() throws Exception {
        Database database = Database.of(System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME));
        assertNotNull(database);
        Metadata metadata = database.getMetadata();
        assertNotNull(metadata);
        assertNotNull(database.getSqlDriver());
        Optional<Map<String, JdbcColumn>> ifAssetsColumns = metadata.getTableColumns("AsseTs");
        assertTrue(ifAssetsColumns.isPresent());
        Map<String, JdbcColumn> assetsColumns = ifAssetsColumns.get();
        JdbcColumn FielD3 = assetsColumns.get("FielD3");
        JdbcColumn fiElD3 = assetsColumns.get("fiElD3");
        JdbcColumn FIelD3 = assetsColumns.get("FielD3");
        assertSame(FielD3, fiElD3);
        assertSame(FielD3, FIelD3);
    }

    @Test
    public void assetsColumns() throws Exception {
        Database database = Database.of(System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME));
        assertNotNull(database);
        Metadata metadata = database.getMetadata();
        assertNotNull(metadata);
        assertNotNull(database.getSqlDriver());
        Optional<Map<String, JdbcColumn>> ifAssetsColumns = metadata.getTableColumns("AsseTs");
        assertTrue(ifAssetsColumns.isPresent());
        Map<String, JdbcColumn> assetsColumns = ifAssetsColumns.get();
        assertEquals(7, assetsColumns.size());
        assertArrayEquals(new String[]{
                "id",
                "name",
                "field3",
                "field4",
                "field5",
                "field6",
                "field7"
        }, assetsColumns.keySet().toArray(new String[]{}));
        JdbcColumn id = assetsColumns.get("id");
        assertEquals("assets", id.getTableName().toLowerCase());
        assertEquals("public", id.getSchemaName().toLowerCase());
        assertSame(Types.DECIMAL, id.getJdbcType());
        JdbcColumn name = assetsColumns.get("name");
        assertSame(Types.VARCHAR, name.getJdbcType());
        JdbcColumn field5 = assetsColumns.get("field5");
        assertSame(Types.DECIMAL, field5.getJdbcType());
        JdbcColumn field6 = assetsColumns.get("field6");
        assertSame(Types.DECIMAL, field6.getJdbcType());
        JdbcColumn field7 = assetsColumns.get("field7");
        assertSame(Types.DOUBLE, field7.getJdbcType());
    }

    @Test
    public void assetsPrimaryKeys() throws Exception {
        Database database = Database.of(System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME));
        assertNotNull(database);
        Metadata metadata = database.getMetadata();
        assertNotNull(metadata);
        assertNotNull(database.getSqlDriver());
        Optional<Map<String, JdbcColumn>> ifAssetsColumns = metadata.getTableColumns("AsseTs");
        Map<String, JdbcColumn> assetsColumns = ifAssetsColumns.get();
        JdbcColumn id = assetsColumns.get("id");
        assertTrue(id.isPk());
        assertFalse(id.isFk());
    }

    @Test
    public void defaultSchema() throws Exception {
        Database database = Database.of(System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME));
        assertEquals("public", database.getMetadata().getDefaultSchema().toLowerCase());
        assertTrue(database.getMetadata().containsTable("AsseTs"));
        assertTrue(database.getMetadata().containsTable("pubLiC.AsseTs"));
        assertEquals(
                database.getMetadata().getTableColumns("AsseTs").get(),
                database.getMetadata().getTableColumns("pubLiC.AsseTs").get()
        );
        database.getMetadata().refreshTable("AsseTs");
        database.getMetadata().refreshTable("PUBLiC.AsseTs");
        assertTrue(database.getMetadata().containsTable("AsseTs"));
        assertTrue(database.getMetadata().containsTable("pubLiC.AsseTs"));
        assertEquals(
                database.getMetadata().getTableColumns("AsseTs").get(),
                database.getMetadata().getTableColumns("pubLiC.AsseTs").get()
        );
    }

    @Test
    public void assetGroupsForeignKeys() throws Exception {
        Database database = Database.of(System.getProperty(TestDataSource.DATA_SOURCE_PROP_NAME));
        assertNotNull(database);
        Metadata metadata = database.getMetadata();
        assertNotNull(metadata);
        assertNotNull(database.getSqlDriver());
        Optional<Map<String, JdbcColumn>> ifAssetGroupsColumns = metadata.getTableColumns("asset_groups");
        Map<String, JdbcColumn> assetGroupsColumns = ifAssetGroupsColumns.get();
        JdbcColumn id = assetGroupsColumns.get("id");
        assertTrue(id.isPk());
        assertFalse(id.isFk());
        JdbcColumn p_id = assetGroupsColumns.get("p_id");
        assertFalse(p_id.isPk());
        assertTrue(p_id.isFk());
        ForeignKey fk = p_id.getFk();
        assertFalse(fk.isDeferrable());
        assertSame(fk.getDeleteRule(), ForeignKey.ForeignKeyRule.CASCADE);
        assertSame(fk.getUpdateRule(), ForeignKey.ForeignKeyRule.NO_ACTION);
        assertEquals("p_id", fk.getField().toLowerCase());
        assertEquals("asset_groups", fk.getReferee().getTable().toLowerCase());
        assertEquals("id", fk.getReferee().getField().toLowerCase());
        assertFalse(fk.getReferee().getCName().isEmpty());
        assertEquals("public", fk.getReferee().getSchema().toLowerCase());
    }

    @Test
    public void foreignKeyRule() {
        assertSame(ForeignKey.ForeignKeyRule.valueOf("CASCADE"), ForeignKey.ForeignKeyRule.CASCADE);
        assertSame(ForeignKey.ForeignKeyRule.valueOf("NO_ACTION"), ForeignKey.ForeignKeyRule.NO_ACTION);
        assertSame(ForeignKey.ForeignKeyRule.valueOf("SET_DEFAULT"), ForeignKey.ForeignKeyRule.SET_DEFAULT);
        assertSame(ForeignKey.ForeignKeyRule.valueOf("SET_NULL"), ForeignKey.ForeignKeyRule.SET_NULL);
        assertSame(ForeignKey.ForeignKeyRule.valueOf((short) DatabaseMetaData.importedKeyCascade), ForeignKey.ForeignKeyRule.CASCADE);
        assertSame(ForeignKey.ForeignKeyRule.valueOf((short) DatabaseMetaData.importedKeyNoAction), ForeignKey.ForeignKeyRule.NO_ACTION);
        assertSame(ForeignKey.ForeignKeyRule.valueOf((short) DatabaseMetaData.importedKeyRestrict), ForeignKey.ForeignKeyRule.NO_ACTION);
        assertSame(ForeignKey.ForeignKeyRule.valueOf((short) DatabaseMetaData.importedKeySetDefault), ForeignKey.ForeignKeyRule.SET_DEFAULT);
        assertSame(ForeignKey.ForeignKeyRule.valueOf((short) DatabaseMetaData.importedKeySetNull), ForeignKey.ForeignKeyRule.SET_NULL);
        assertNull(ForeignKey.ForeignKeyRule.valueOf((short) DatabaseMetaData.importedKeyInitiallyDeferred));
        assertArrayEquals(new ForeignKey.ForeignKeyRule[]{
                ForeignKey.ForeignKeyRule.NO_ACTION,
                ForeignKey.ForeignKeyRule.SET_NULL,
                ForeignKey.ForeignKeyRule.SET_DEFAULT,
                ForeignKey.ForeignKeyRule.CASCADE
        }, ForeignKey.ForeignKeyRule.values());
    }
}
