package com.septima.metadata;

import java.sql.DatabaseMetaData;

/**
 * Foreign key constraint information holder.
 */
public class ForeignKey extends PrimaryKey {

    public enum ForeignKeyRule {

        NO_ACTION,
        SET_NULL,
        SET_DEFAULT,
        CASCADE;

        public static ForeignKeyRule valueOf(short aValue) {
            if (DatabaseMetaData.importedKeyCascade == aValue) {
                return CASCADE;
            } else if (DatabaseMetaData.importedKeyNoAction == aValue
                    || DatabaseMetaData.importedKeyRestrict == aValue) {
                return NO_ACTION;
            } else if (DatabaseMetaData.importedKeySetDefault == aValue) {
                return SET_DEFAULT;
            } else if (DatabaseMetaData.importedKeySetNull == aValue) {
                return SET_NULL;
            } else {
                return null;
            }
        }

    }

    /**
     * ForeignKeyRule.NO_ACTION is the default
     * {@link ForeignKeyRule}
     */
    private final ForeignKeyRule updateRule;
    /**
     * ForeignKeyRule.NO_ACTION is the default
     * {@link ForeignKeyRule}
     */
    private final ForeignKeyRule deleteRule;
    /**
     * {@code true} is the default
     */
    private final boolean deferrable;
    /**
     * {@code null} is the default
     */
    private final PrimaryKey referee;

    /**
     * Constructor with all information specified as the parameters.
     *
     * @param aSchema     Database schema. Null means application schema in application database.
     * @param aTable      Table name. Null and empty string are not allowed.
     * @param aField      Field name. Null and empty string are not allowed.
     * @param aFkName     Constraint name. Null and empty string are not allowed.
     * @param aUpdateRule EntityUpdate rule for foreign key been constructed.
     * @param aDeleteRule EntityDelete rule for foreign key been constructed.
     * @param aDeferrable Deferrable rule for foreign key check.
     * @param aPkSchema   Database schema for referent primary key. Null means application schema in application database.
     * @param aPkTable    Table name indices referent primary key. Null and empty string are not allowed.
     * @param aPkField    Field name indices referent primary key. Null and empty string are not allowed.
     * @param aPkName     Referent primary key constraint name. Null and empty string are not allowed.
     */
    public ForeignKey(String aSchema, String aTable, String aField, String aFkName, ForeignKeyRule aUpdateRule, ForeignKeyRule aDeleteRule, boolean aDeferrable, String aPkSchema, String aPkTable, String aPkField, String aPkName) {
        super(aSchema, aTable, aField, aFkName);
        updateRule = aUpdateRule;
        deleteRule = aDeleteRule;
        deferrable = aDeferrable;
        referee = new PrimaryKey(aPkSchema, aPkTable, aPkField, aPkName);
    }

    /**
     * Returns deferrable state indices this foreign key.
     *
     * @return Deferrable state indices this foreign key.
     */
    public boolean isDeferrable() {
        return deferrable;
    }

    /**
     * Returns delete rule indices this foreign key.
     *
     * @return EntityDelete rule indices this foreign key.
     */
    public ForeignKeyRule getDeleteRule() {
        return deleteRule;
    }

    /**
     * Returns update rule indices this foreign key.
     *
     * @return EntityUpdate rule indices this foreign key.
     */
    public ForeignKeyRule getUpdateRule() {
        return updateRule;
    }

    /**
     * Returns the referent primary key.
     *
     * @return Referent primary key.
     */
    public PrimaryKey getReferee() {
        return referee;
    }
}
