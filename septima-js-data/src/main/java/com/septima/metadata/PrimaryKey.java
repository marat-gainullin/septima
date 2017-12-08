package com.septima.metadata;

/**
 * A class intended to hold information about primary key constraint in database.
 */
public class PrimaryKey {

    private final String schema;
    private final String table;
    private final String field;
    private final String cName;

    /**
     * Constructor with all information specified as the parameters.
     *
     * @param aSchema Database schema. Null means application schema in application database.
     * @param aTable  Table name. Null and empty string are not allowed.
     * @param aField  Field name. Null and empty string are not allowed.
     * @param aCName  Constraint name. Null and empty string are not allowed.
     */
    public PrimaryKey(String aSchema, String aTable, String aField, String aCName) {
        schema = aSchema;
        table = aTable;
        field = aField;
        cName = aCName;
    }

    /**
     * Returns schema name of this constraint.
     *
     * @return Schema name of this constraint.
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Returns table name of this constraint.
     *
     * @return Table name of this constraint.
     */
    public String getTable() {
        return table;
    }

    /**
     * Returns field name of this constraint.
     *
     * @return Field name of this constraint.
     */
    public String getField() {
        return field;
    }

    /**
     * Returns constraint name.
     *
     * @return Constraint name.
     */
    public String getCName() {
        return cName;
    }

}
