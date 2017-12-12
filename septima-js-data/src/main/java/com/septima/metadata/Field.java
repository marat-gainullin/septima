package com.septima.metadata;

import com.septima.ApplicationTypes;

/**
 * This class is table field representation. It holds information about field
 * name, description, typeInfo, size and information about primary and foreign
 * keys. If <code>isPk()</code> returns true, than this field is the primary key
 * in corresponding table. If <code>getFk()</code> returns reference to a
 * <code>PrimaryKey</code>, than it is a foreign key in corresponding table,
 * and it references to returning <code>PrimaryKey</code>.
 *
 * @author mg
 */
public class Field {

    private final String name;
    private final String description;
    /**
     * In queries, such as select t1.f1 as f11, t2.f1 as f21 to preserve output fields' names unique,
     * but be able to generate right update sql clauses for multiple tables.
     */
    private final String originalName;
    private final String tableName;
    /**
     * Null value will be used as "unknown" type
     */
    private final String type;
    /**
     * true is the default
     */
    private final boolean nullable;
    private final boolean pk;
    private final ForeignKey fk;

    /**
     * Constructor with name.
     *
     * @param aName Name of the created field.
     */
    public Field(String aName) {
        this(aName, null);
    }

    /**
     * Constructor with name and description.
     *
     * @param aName        Name of the created field.
     * @param aDescription Description of the created field.
     */
    public Field(String aName, String aDescription) {
        this(aName, aDescription, ApplicationTypes.STRING_TYPE_NAME);
    }

    public Field(String aName, String aDescription, String aType) {
        this(aName, aDescription, aName, null, aType, true, false, null);
    }

    public Field(
            String aName,
            String aDescription,
            String aOriginalName,
            String aTableName,
            String aType,
            boolean aNullable,
            boolean aPk,
            ForeignKey aFk
    ) {
        name = aName;
        description = aDescription;
        originalName = aOriginalName;
        tableName = aTableName;
        type = aType;
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
     * Returns if this field is foreign key to another table or it is a
     * self-reference key.
     *
     * @return If this field is foreign key to another table or it is
     * self-reference key.
     */
    public boolean isFk() {
        return fk != null;
    }

    /**
     * Returns if this field is primary key.
     *
     * @return If this field is primary key.
     */
    public boolean isPk() {
        return pk;
    }

    /**
     * Returns foreign key specification of this field if it references to some
     * table.
     *
     * @return Foreign key specification of this field if it references to some
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
     * Returns the field's type information
     *
     * @return The field's type information
     */
    public String getType() {
        return type;
    }


    /*
    public Object generateValue() {
        Object value;
        if (type != null) {
            switch (type) {
                case ApplicationTypes.NUMBER_TYPE_NAME:
                    value = IdGenerator.genId();
                    break;
                case ApplicationTypes.STRING_TYPE_NAME:
                    value = IdGenerator.genStringId();
                    break;
                case ApplicationTypes.DATE_TYPE_NAME:
                    value = new Date();
                    break;
                case ApplicationTypes.BOOLEAN_TYPE_NAME:
                    value = false;
                    break;
                default:
                    value = null;
                    break;
            }
        } else {
            value = null;
        }
        return value;
    }
    */

    /**
     * Returns whether this field is nullable.
     *
     * @return Whether this field is nullable.
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
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
            if (rf.getSchema() != null && !rf.getSchema().isEmpty()) {
                sb.append(rf.getSchema()).append(".");
            }
            if (rf.getTable() != null && !rf.getTable().isEmpty()) {
                sb.append(rf.getTable()).append(".");
            }
            sb.append(rf.getField());
        }
        sb.append(", ").append(type);
        if (nullable) {
            sb.append(", nullable");
        }
        return sb.toString();
    }
}
