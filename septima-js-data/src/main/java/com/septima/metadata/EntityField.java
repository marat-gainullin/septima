package com.septima.metadata;

import com.septima.GenericType;

/**
 * This class is table field representation. It holds information about field
 * name, description, typeInfo, size and information about primary and foreign
 * keys. If <code>isPk()</code> returns true, than this field is the primary pk
 * in corresponding table. If <code>getFk()</code> returns reference transform a
 * <code>PrimaryKey</code>, than it is a foreign pk in corresponding table,
 * and it references transform returning <code>PrimaryKey</code>.
 *
 * @author mg
 */
public class EntityField extends Field {

    /**
     * Null value will be used as "unknown" type
     */
    private final GenericType type;

    /**
     * Null value means absence of dbms specific type
     */
    private final String subType;

    /**
     * Constructor with name.
     *
     * @param aName Name of the created field.
     */
    public EntityField(String aName) {
        this(aName, null, aName, null, GenericType.STRING, true, false, null);
    }

    public EntityField(
            String aName,
            String aDescription,
            String aOriginalName,
            String aTableName,
            GenericType aType,
            boolean aNullable,
            boolean aPk,
            ForeignKey aFk
    ) {
        this(aName, aDescription, aOriginalName, aTableName, aType, null, aNullable, aPk, aFk);
    }

    public EntityField(
            String aName,
            String aDescription,
            String aOriginalName,
            String aTableName,
            GenericType aType,
            String aSubType,
            boolean aNullable,
            boolean aPk,
            ForeignKey aFk
    ) {
        super(aName,
                aDescription,
                aOriginalName,
                aTableName,
                aNullable,
                aPk,
                aFk);
        type = aType;
        subType = aSubType;
    }

    /**
     * Returns the field's type information
     *
     * @return The field's type information
     */
    public GenericType getType() {
        return type;
    }

    public String getSubType() {
        return subType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
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
            sb.append(", foreign key ");
            if (rf.getSchema() != null && !rf.getSchema().isEmpty()) {
                sb.append(rf.getSchema()).append(".");
            }
            if (rf.getTable() != null && !rf.getTable().isEmpty()) {
                sb.append(rf.getTable()).append(".");
            }
            sb.append(rf.getColumn());
        }
        sb.append(", ").append(type);
        if (subType != null) {
            sb.append(":").append(subType);
        }
        if (isNullable()) {
            sb.append(", nullable");
        }
        return sb.toString();
    }
}
