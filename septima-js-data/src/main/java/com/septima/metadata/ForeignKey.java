package com.septima.metadata;

/**
 * Foreign key constraint information holder.
 */
public class ForeignKey extends PrimaryKey {

    public enum ForeignKeyRule {

        NOACTION,
        SETNULL,
        SETDEFAULT,
        CASCADE;

        public static ForeignKeyRule valueOf(short aValue) {
            if (/*DatabaseMetaData.importedKeyCascade*/0 == aValue) {
                return CASCADE;
            } else if (/*DatabaseMetaData.importedKeyNoAction*/3 == aValue
                    || /*DatabaseMetaData.importedKeyRestrict*/1 == aValue) {
                return NOACTION;
            } else if (/*DatabaseMetaData.importedKeySetDefault*/4 == aValue) {
                return SETDEFAULT;
            } else if (/*DatabaseMetaData.importedKeySetNull*/2 == aValue) {
                return SETNULL;
            } else {
                return null;
            }
        }

        public short toShort() {
            if (this == CASCADE) {
                return 0;//DatabaseMetaData.importedKeyCascade;
            } else if (this == NOACTION) {
                return 3;//DatabaseMetaData.importedKeyNoAction;
            } else if (this == SETDEFAULT) {
                return 4;//DatabaseMetaData.importedKeySetDefault;
            } else // if(this == SETNULL)
            {
                return 2;//DatabaseMetaData.importedKeySetNull;
            }
        }
    }

    /**
     * ForeignKeyRule.NOACTION is the default
     * {@link ForeignKeyRule}
     */
    private final ForeignKeyRule updateRule;
    /**
     * ForeignKeyRule.NOACTION is the default
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
     * @param aSchema Database schema. Null means application schema in application database.
     * @param aTable Table name. Null and empty string are not allowed.
     * @param aField Field name. Null and empty string are not allowed.
     * @param aFkName Constraint name. Null and empty string are not allowed.
     * @param aUpdateRule Update rule for foreign key been constructed.
     * @param aDeleteRule Delete rule for foreign key been constructed.
     * @param aDeferrable Deferrable rule for foreign key check.
     * @param aPkSchema Database schema for referent primary key. Null means application schema in application database.
     * @param aPkTable Table name of referent primary key. Null and empty string are not allowed.
     * @param aPkField Field name of referent primary key. Null and empty string are not allowed.
     * @param aPkName Referent primary key constraint name. Null and empty string are not allowed.
     */
    public ForeignKey(String aSchema, String aTable, String aField, String aFkName, ForeignKeyRule aUpdateRule, ForeignKeyRule aDeleteRule, boolean aDeferrable, String aPkSchema, String aPkTable, String aPkField, String aPkName) {
        super(aSchema, aTable, aField, aFkName);
        updateRule = aUpdateRule;
        deleteRule = aDeleteRule;
        deferrable = aDeferrable;
        referee = new PrimaryKey(aPkSchema, aPkTable, aPkField, aPkName);
    }

    /**
     * Returns deferrable state of this foreign key.
     * @return Deferrable state of this foreign key.
     */
    public boolean getDeferrable() {
        return deferrable;
    }

    /**
     * Returns delete rule of this foreign key.
     * @return Delete rule of this foreign key.
     */
    public ForeignKeyRule getDeleteRule() {
        return deleteRule;
    }

    /**
     * Returns update rule of this foreign key.
     * @return Update rule of this foreign key.
     */
    public ForeignKeyRule getUpdateRule() {
        return updateRule;
    }

    /**
     * Returns the referent primary key.
     * @return Referent primary key.
     */
    public PrimaryKey getReferee() {
        return referee;
    }
}
