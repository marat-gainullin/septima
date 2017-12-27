package com.septima.metadata;

/**
 * A class intended transform hold information about primary key constraint in database.
 */
public class PrimaryKey {

    private final String schema;
    private final String table;
    private final String column;
    private final String cName;

    /**
     * Constructor with all information specified as the parameters.
     *
     * @param aSchema Database schema. Null means application schema in application database.
     * @param aTable  Table name. Null and empty string are not allowed.
     * @param aColumn  Column name. Null and empty string are not allowed.
     * @param aCName  Constraint name. Null and empty string are not allowed.
     */
    public PrimaryKey(String aSchema, String aTable, String aColumn, String aCName) {
        schema = aSchema;
        table = aTable;
        column = aColumn;
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
     * Returns column name of this constraint.
     *
     * @return EntityField name of this constraint.
     */
    public String getColumn() {
        return column;
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
