package com.septima.metadata;

import java.util.Map;

/**
 * @author mg
 */
public class TableKeys {

    private final String tableName;
    private final Map<String, PrimaryKey> pks;
    private final Map<String, ForeignKey> fks;

    /**
     * Constructor with table name
     *
     * @param aTableName Table name without any schema name.
     */
    public TableKeys(String aTableName) {
        this(aTableName, Map.of(), Map.of());
    }

    public TableKeys(String aTableName, Map<String, PrimaryKey> aPks, Map<String, ForeignKey> aFks) {
        tableName = aTableName;
        pks = aPks;
        fks = aFks;
    }

    /**
     * Returns table name without any schema name.
     *
     * @return Table name without any schema name.
     */
    public String getTableName() {
        return tableName;
    }

    public boolean isEmpty() {
        return (pks == null || pks.isEmpty()) && (fks == null || fks.isEmpty());
    }

    public Map<String, ForeignKey> getFks() {
        return fks;
    }

    public Map<String, PrimaryKey> getPks() {
        return pks;
    }

    /*
    public static boolean isKeysCompatible(TableKeys table1Keys, String field1Name, TableKeys table2Keys, String field2Name) {
        if (table1Keys != null && field1Name != null
                && table2Keys != null && field2Name != null) {
            PrimaryKey[] lKeys = new PrimaryKey[2];
            int lKeysCount = 0;
            Map<String, PrimaryKey> lPks = table1Keys.getPks();
            Map<String, ForeignKey> lFks = table1Keys.getFks();
            if (lPks.containsKey(field1Name)) {
                PrimaryKey lfoundPk = lPks.get(field1Name);
                lKeys[lKeysCount] = new PrimaryKey(lfoundPk.getSchema(), lfoundPk.getTable(), lfoundPk.getField(), lfoundPk.getCName());
                lKeysCount++;
            }
            if (lFks.containsKey(field1Name)) {
                ForeignKey lfoundFk = lFks.get(field1Name);
                lKeys[lKeysCount] = new PrimaryKey(lfoundFk.getReferee().getSchema(), lfoundFk.getReferee().getTable(), lfoundFk.getReferee().getField(), lfoundFk.getReferee().getCName());
                lKeysCount++;
            }
            PrimaryKey[] rKeys = new PrimaryKey[2];
            int rKeysCount = 0;
            Map<String, ForeignKey> rFks = table2Keys.getFks();
            Map<String, PrimaryKey> rPks = table2Keys.getPks();
            if (rPks.containsKey(field2Name)) {
                PrimaryKey lfoundPk = rPks.get(field2Name);
                rKeys[rKeysCount] = new PrimaryKey(lfoundPk.getSchema(), lfoundPk.getTable(), lfoundPk.getField(), lfoundPk.getCName());
                rKeysCount++;
            }
            if (rFks.containsKey(field2Name)) {
                ForeignKey lfoundFk = rFks.get(field2Name);
                rKeys[rKeysCount] = new PrimaryKey(lfoundFk.getReferee().getSchema(), lfoundFk.getReferee().getTable(), lfoundFk.getReferee().getField(), lfoundFk.getReferee().getCName());
                rKeysCount++;
            }
            for (int i = 0; i < lKeysCount; i++) {
                for (int j = 0; j < rKeysCount; j++) {
                    String lSchema = lKeys[i].getSchema();
                    if (lSchema == null) {
                        lSchema = "";
                    }
                    lSchema = lSchema.toUpperCase();
                    String lTable = lKeys[i].getTable();
                    if (lTable == null) {
                        lTable = "";
                    }
                    lTable = lTable.toUpperCase();
                    String lField = lKeys[i].getField();
                    if (lField == null) {
                        lField = "";
                    }
                    lField = lField.toUpperCase();

                    String rSchema = rKeys[j].getSchema();
                    if (rSchema == null) {
                        rSchema = "";
                    }
                    rSchema = rSchema.toUpperCase();
                    String rTable = rKeys[j].getTable();
                    if (rTable == null) {
                        rTable = "";
                    }
                    rTable = rTable.toUpperCase();
                    String rField = rKeys[j].getField();
                    if (rField == null) {
                        rField = "";
                    }
                    rField = rField.toUpperCase();

                    if (lSchema.equals(rSchema)
                            && lTable.equals(rTable)
                            && lField.equals(rField)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
    */
}
