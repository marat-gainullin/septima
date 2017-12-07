package com.septima.metadata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 *
 * @author mg
 */
public class TableKeys {

    protected Map<String, PrimaryKey> pks = new HashMap<>();
    protected Map<String, ForeignKey> fks = new HashMap<>();
    protected String tableName;

    private TableKeys(TableKeys aSource) {
        this();
        if (aSource != null) {
            String otherTableName = aSource.getTableName();
            if (otherTableName != null) {
                tableName = new String(otherTableName.toCharArray());
            }
            fks.clear();
            Map<String, ForeignKey> lfks = aSource.getFks();
            Set<Entry<String, ForeignKey>> lfkSet = lfks.entrySet();
            if (lfkSet != null) {
                Iterator<Entry<String, ForeignKey>> lfkIt = lfkSet.iterator();
                while (lfkIt.hasNext()) {
                    Entry<String, ForeignKey> lfkEntry = lfkIt.next();
                    String lfkFieldName = lfkEntry.getKey();
                    ForeignKey lfkSpec = lfkEntry.getValue();
                    fks.put(new String(lfkFieldName.toCharArray()), (ForeignKey) lfkSpec.copy());
                }
            }
            pks.clear();
            Map<String, PrimaryKey> lpks = aSource.getPks();
            Set<Entry<String, PrimaryKey>> lpkSet = lpks.entrySet();
            if (lpkSet != null) {
                Iterator<Entry<String, PrimaryKey>> lpkIt = lpkSet.iterator();
                while (lpkIt.hasNext()) {
                    Entry<String, PrimaryKey> lpkEntry = lpkIt.next();
                    String lpkFieldName = lpkEntry.getKey();
                    PrimaryKey lpkSpec = lpkEntry.getValue();
                    pks.put(new String(lpkFieldName.toCharArray()), lpkSpec.copy());
                }
            }
        }
    }

    public boolean isPrimaryKey(String fieldName) {
        return (pks != null && pks.containsKey(fieldName));
    }

    public boolean isForeignKey(String fieldName) {
        return (fks != null && fks.containsKey(fieldName));
    }

    public TableKeys() {
        super();
    }

    /**
     * Constructor with table name
     * @param aTableName Table name without any schema name.
     */
    public TableKeys(String aTableName) {
        super();
        tableName = aTableName;
    }

    /**
     * Returns table name without any schema name.
     * @return Table name without any schema name.
     */
    public String getTableName() {
        return tableName;
    }

    public void addForeignKey(String aFkSchema, String aFkTable, String aFkField, String aFkName, ForeignKey.ForeignKeyRule afkUpdateRule, ForeignKey.ForeignKeyRule afkDeleteRule, boolean afkDeferrable, String aPkSchema, String aPkTable, String aPkField, String aPkName) {
        fks.put(aFkField, new ForeignKey(aFkSchema, aFkTable, aFkField, aFkName, afkUpdateRule, afkDeleteRule, afkDeferrable, aPkSchema, aPkTable, aPkField, aPkName));
    }

    public void addPrimaryKey(String aPkSchema, String aPkTable, String aPkField, String aPkName) {
        pks.put(aPkField, new PrimaryKey(aPkSchema, aPkTable, aPkField, aPkName));
    }

    public void clear() {
        fks.clear();
        pks.clear();
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

    public TableKeys copy() {
        TableKeys dbTblFks = new TableKeys(this);
        return dbTblFks;
    }

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
}
