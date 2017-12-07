package com.septima.metadata;

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

    protected String name = "";
    // In queries, such as select t1.f1 as f11, t2.f1 as f21 to preserve output fields' names unique,
    // but be able to generate right update sql clauses for multiple tables.
    protected String originalName = "";
    protected String tableName;
    protected String description;
    protected String type;// Null value will be used as "unknown" type
    protected boolean readonly;
    protected boolean nullable = true;
    protected boolean pk;
    protected ForeignKey fk;

    /**
     * The default constructor.
     */
    public Field() {
        super();
    }

    /**
     * Constructor with name.
     *
     * @param aName Name of the created field.
     */
    public Field(String aName) {
        this();
        name = aName;
    }

    /**
     * Constructor with name and description.
     *
     * @param aName        Name of the created field.
     * @param aDescription Description of the created field.
     */
    public Field(String aName, String aDescription) {
        this(aName);
        description = aDescription;
    }

    /**
     * Constructor with name, description and typeInfo.
     *
     * @param aName        Name of the created field.
     * @param aDescription Description of the created field.
     * @param aType        Type name of the created field.
     */
    public Field(String aName, String aDescription, String aType) {
        this(aName, aDescription);
        type = aType;
    }

    public String getOriginalName() {
        return originalName;
    }

    public void setOriginalName(String aValue) {
        originalName = aValue;
    }

    public String getTableName() {
        return tableName;
    }

    /**
     * Sets table name.
     *
     * @param aValue The table name to be set.
     */
    public void setTableName(String aValue) {
        tableName = aValue;
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
     * Sets primary key state of this field.
     *
     * @param aValue Flag, indicating primary key state of this field.
     */
    public void setPk(boolean aValue) {
        pk = aValue;
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
     * Sets foreign key specification to this field, making it the reference to
     * some table.
     *
     * @param aValue Foreign key specification to be set to this field.
     */
    public void setFk(ForeignKey aValue) {
        fk = aValue;
    }

    /**
     * Returns if this field is readonly.
     *
     * @return If true this field is readonly.
     */
    public boolean isReadonly() {
        return readonly;
    }

    /**
     * Sets readonly flag to this field.
     *
     * @param aValue Flag to be set to this field.
     */
    public void setReadonly(boolean aValue) {
        readonly = aValue;
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
     * Set the name to this field.
     *
     * @param aValue A name to be set.
     */
    public void setName(String aValue) {
        name = aValue;
    }

    private static final String DESCRIPTION_JS_DOC = ""
            + "/**\n"
            + " * The description of the field.\n"
            + " */";

    /**
     * Returns description of the field.
     *
     * @return Description of the field.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Set the description to this field.
     *
     * @param aValue A description to be set.
     */
    public void setDescription(String aValue) {
        description = aValue;
    }

    /**
     * Returns the field's type information
     *
     * @return The field's type information
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the field's type description
     *
     * @param aValue The filed's type description
     */
    public void setType(String aValue) {
        type = aValue;
    }

    /*
    public Object generateValue() {
        Object value;
        if (type != null) {
            switch (type) {
                case Constants.NUMBER_TYPE_NAME:
                    value = IdGenerator.genId();
                    break;
                case Constants.STRING_TYPE_NAME:
                    value = IdGenerator.genStringId();
                    break;
                case Constants.DATE_TYPE_NAME:
                    value = new Date();
                    break;
                case Constants.BOOLEAN_TYPE_NAME:
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
     * Sets the field's nullable state.
     *
     * @param aValue Field's nullable flag.
     */
    public void setNullable(boolean aValue) {
        nullable = aValue;
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
            if (rf.schema != null && !rf.schema.isEmpty()) {
                sb.append(rf.schema).append(".");
            }
            if (rf.table != null && !rf.table.isEmpty()) {
                sb.append(rf.table).append(".");
            }
            sb.append(rf.field);
        }
        sb.append(", ").append(type);
        if (nullable) {
            sb.append(", nullable");
        }
        if (readonly) {
            sb.append(", readonly");
        }
        return sb.toString();
    }
}
