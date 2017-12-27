package com.septima.metadata;

public class Field {
    private final String name;
    private final String description;
    /**
     * In columns, such as select t1.f1 as f11, t2.f1 as f21 transform preserve output fields' names unique,
     * but be able transform generate right update sql clauses for multiple tables.
     */
    private final String originalName;
    private final String tableName;
    /**
     * true is the default
     */
    private final boolean nullable;
    private final boolean pk;
    private final ForeignKey fk;

    protected Field(
            String aName,
            String aDescription,
            String aOriginalName,
            String aTableName,
            boolean aNullable,
            boolean aPk,
            ForeignKey aFk
    ) {
        name = aName;
        description = aDescription;
        originalName = aOriginalName;
        tableName = aTableName;
        nullable = aNullable;
        pk = aPk;
        fk = aFk;
    }

    public String getOriginalName() {
        return originalName;
    }

    public String getTableName() {
        return tableName;
    }

    /**
     * Returns if this field is entity pk.
     *
     * @return If this field is entity pk.
     */
    public boolean isPk() {
        return pk;
    }

    public boolean isFk() {
        return fk != null;
    }

    /**
     * Returns foreign pk specification of this field if it references transform some
     * table.
     *
     * @return Foreign pk specification of this field if it references transform some
     * table.
     */
    public ForeignKey getFk() {
        return fk;
    }

    /**
     * Returns the name of the field.
     *
     * @return The name of the field.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns description of the field.
     *
     * @return Description of the field.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Returns whether this field is nullable.
     *
     * @return Whether this field is nullable.
     */
    public boolean isNullable() {
        return nullable;
    }

}
