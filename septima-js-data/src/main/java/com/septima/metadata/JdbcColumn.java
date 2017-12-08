package com.septima.metadata;

/**
 * @author mg
 */
public class JdbcColumn extends Field {

    private final int size;
    private final int scale;
    private final int precision;
    /**
     * {@code true} is the default.
     */
    private final boolean signed;
    private final String schemaName;
    private final int jdbcType;

    public JdbcColumn(
            String aName,
            String aDescription,
            String aOriginalName,
            String aTableName,
            String aType,
            boolean aNullable,
            boolean aPk,
            ForeignKey aFk,
            int aSize,
            int aScale,
            int aPrecision,
            boolean aSigned,
            String aSchemaName,
            int aJdbcType
    ) {
        super(
                aName,
                aDescription,
                aOriginalName,
                aTableName,
                aType,
                aNullable,
                aPk,
                aFk
        );
        size = aSize;
        scale = aScale;
        precision = aPrecision;
        signed = aSigned;
        schemaName = aSchemaName;
        jdbcType = aJdbcType;
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
     * Returns the field size.
     *
     * @return The field size.
     */
    public int getSize() {
        return size;
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
     * Returns the field's precision.
     *
     * @return The field's precision.
     */
    public int getPrecision() {
        return precision;
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
     * Returns the field jdbc type.
     *
     * @return The field jdbc type.
     */
    public int getJdbcType() {
        return jdbcType;
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
        if (getTableName() != null && !getTableName().isEmpty()) {
            sb.append(getTableName()).append(".");
        }
        if (getOriginalName() != null && !getOriginalName().isEmpty()) {
            sb.append(getOriginalName());
        } else {
            sb.append(getName());
        }
        if (getDescription() != null && !getDescription().isEmpty()) {
            sb.append(" (").append(getDescription()).append(")");
        }
        if (isPk()) {
            sb.append(", primary key");
        }
        if (getFk() != null && getFk().getReferee() != null) {
            PrimaryKey rf = getFk().getReferee();
            sb.append(", foreign key to ");
            if (rf.getSchema() != null && !rf.getSchema().isEmpty()) {
                sb.append(rf.getSchema()).append(".");
            }
            if (rf.getTable() != null && !rf.getTable().isEmpty()) {
                sb.append(rf.getTable()).append(".");
            }
            sb.append(rf.getField());
        }
        sb.append(", ").append(getType());
        sb.append(", size ").append(size).append(", precision ").append(precision).append(", scale ").append(scale);
        if (signed) {
            sb.append(", signed");
        }
        if (isNullable()) {
            sb.append(", nullable");
        }
        return sb.toString();
    }
}
