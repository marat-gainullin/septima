package com.septima.metadata;

/**
 *
 * @author mg
 */
public class JdbcColumn extends Field {

    private int size;
    private int scale;
    private int precision;
    private boolean signed = true;
    private String schemaName;
    private int jdbcType;

    public JdbcColumn() {
        super();
    }
    
    public JdbcColumn(String aName) {
        super();
        name = aName;
    }
    
    /**
     * Returns the field's schema name.
     *
     * @return The field's schema name.
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * Sets this field schema name.
     *
     * @param aValue This field schema name.
     */
    public void setSchemaName(String aValue) {
        schemaName = aValue;
    }

    /**
     * Returns the field size.
     *
     * @return The field size.
     */
    public int getSize() {
        return size;
    }

    /**
     * Sets the field size.
     *
     * @param aValue The field size to be set.
     */
    public void setSize(int aValue) {
        size = aValue;
    }

    /**
     * Returns the field's scale.
     *
     * @return The field's scale.
     */
    public int getScale() {
        return scale;
    }

    /**
     * Sets the field's scale.
     *
     * @param aValue The field's scale to be set.
     */
    public void setScale(int aValue) {
        scale = aValue;
    }

    /**
     * Returns the field's precision.
     *
     * @return The field's precision.
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * Sets the field's precision.
     *
     * @param aValue The field's precision.
     */
    public void setPrecision(int aValue) {
        precision = aValue;
    }

    /**
     * Returns whether this field is signed.
     *
     * @return Whether this field is signed.
     */
    public boolean isSigned() {
        return signed;
    }

    /**
     * Sets the field's signed state.
     *
     * @param aValue Field's signed flag.
     */
    public void setSigned(boolean aValue) {
        signed = aValue;
    }

    /**
     * Returns the field size.
     *
     * @return The field size.
     */
    public int getJdbcType() {
        return jdbcType;
    }

    /**
     * Sets the field size.
     *
     * @param aValue The field size to be set.
     */
    public void setJdbcType(int aValue) {
        jdbcType = aValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (schemaName != null && !schemaName.isEmpty()) {
            sb.append(schemaName).append(".");
        }
        if (tableName != null && !tableName.isEmpty()) {
            sb.append(tableName).append(".");
        }
        if (originalName != null && !originalName.isEmpty()) {
            sb.append(originalName);
        } else {
            sb.append(name);
        }
        if (description != null && !description.isEmpty()) {
            sb.append(" (").append(description).append(")");
        }
        if (pk) {
            sb.append(", primary key");
        }
        if (fk != null && fk.getReferee() != null) {
            PrimaryKey rf = fk.getReferee();
            sb.append(", foreign key to ");
            if (rf.schema != null && !rf.schema.isEmpty()) {
                sb.append(rf.schema).append(".");
            }
            if (rf.table != null && !rf.table.isEmpty()) {
                sb.append(rf.table).append(".");
            }
            sb.append(rf.field);
        }
        sb.append(", ").append(type);
        sb.append(", size ").append(size).append(", precision ").append(precision).append(", scale ").append(scale);
        if (signed) {
            sb.append(", signed");
        }
        if (nullable) {
            sb.append(", nullable");
        }
        if (readonly) {
            sb.append(", readonly");
        }
        return sb.toString();
    }
}
