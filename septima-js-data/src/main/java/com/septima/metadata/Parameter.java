package com.septima.metadata;

import java.sql.ParameterMetaData;

/**
 * This class is a parameter specification for queries.
 * It holds information about field as the <code>Field</code> class
 * and additional information about parameter mode, value and default value.
 *
 * TODO: Remove 'Field' inheritance (add 'name' and 'type' fields explicitly), maybe 'mode' and 'modified' fields too.
 * TODO: Maybe remove this class at all and use Change.Value instead it?
 *
 * @author mg
 */
public class Parameter extends Field {

    private final int mode;
    private boolean modified;

    private Object value;

    /**
     * Constructor with name.
     *
     * @param aName Name of the created parameter.
     */
    public Parameter(String aName) {
        this(aName, null);
    }

    /**
     * Constructor with name and description.
     *
     * @param aName        Name of the created parameter.
     * @param aDescription Description of the created parameter.
     */
    public Parameter(String aName, String aDescription) {
        this(aName, aDescription, null);
    }

    /**
     * Constructor with name, description and typeInfo.
     *
     * @param aName        Name of the created parameter.
     * @param aDescription Description of the created parameter.
     * @param aType        Type name of the created parameter.
     */
    public Parameter(String aName, String aDescription, String aType) {
        this(aName, aDescription, aType, null, false, ParameterMetaData.parameterModeIn);
    }

    public Parameter(String aName, String aDescription, String aType, Object aValue, boolean aModified, int aMode) {
        super(aName, aDescription, aType);
        value = aValue;
        modified = aModified;
        mode = aMode;
    }

    /**
     * Returns parameter's mode.
     *
     * @return Parameter's mode.
     */
    public int getMode() {
        return mode;
    }

    /**
     * Returns parameter's value.
     *
     * @return Parameter's value.
     */
    public Object getValue() {
        return value;
    }

    /**
     * Sets the value of the parameter.
     *
     * @param aValue A value to be set as the parameter's value.
     */
    public void setValue(Object aValue) {
        if (value != aValue) {
            value = aValue;
            modified = true;
        }
    }

    /**
     * Gets parameter's value as a String, whether it feasible. The result
     * exists only for non-null values and some simple types.
     *
     * @return String representing Parameter's value, this value can be used to
     * set the value using <code>setValueByString()</code>.
     * @throws Exception if parameter's value not to be converted.

    public String getValueAsString() throws Exception {
    if (getValue() != null) {
    if (getValue() instanceof java.math.BigDecimal
    || getValue() instanceof Float
    || getValue() instanceof Double
    || getValue() instanceof Short
    || getValue() instanceof Integer
    || getValue() instanceof Boolean
    || getValue() instanceof String) {
    return String.valueOf(getValue());
    } else if (getValue() instanceof java.util.Date) {
    return String.valueOf(((java.util.Date) getValue()).getTime());
    } else {
    throw new IllegalStateException();
    }
    } else {
    throw new IllegalStateException();
    }
    }
     */

    /**
     * Sets the value of the parameter using a String in the format compatible
     * with <code>getValueAsString()</code> method.
     *
     * @throws Exception if operation fails.
    public void setValueByString(String aValue) throws Exception {
    if (aValue != null) {
    if (getType() != null) {
    if (getTypeInfo().javaClassName.equals(String.class.getName())) {
    value = aValue;
    } else if (getTypeInfo().javaClassName.equals(java.math.BigDecimal.class.getName())) {
    value = new BigDecimal(aValue);
    } else if (getTypeInfo().javaClassName.equals(Float.class.getName())) {
    value = Float.valueOf(aValue);
    } else if (getTypeInfo().javaClassName.equals(Double.class.getName())) {
    value = Double.valueOf(aValue);
    } else if (getTypeInfo().javaClassName.equals(Short.class.getName())) {
    value = Short.valueOf(aValue);
    } else if (getTypeInfo().javaClassName.equals(Integer.class.getName())) {
    value = Integer.valueOf(aValue);
    } else if (getTypeInfo().javaClassName.equals(Boolean.class.getName())) {
    value = Boolean.valueOf(aValue);
    } else if (getTypeInfo().javaClassName.equals(java.util.Date.class.getName())) {
    value = new java.util.Date(Long.valueOf(aValue));
    } else {
    throw new IllegalStateException();
    }
    } else {
    throw new IllegalStateException();
    }
    } else {
    throw new IllegalArgumentException("Parameter must not be null."); //NOI18N
    }
    }
     */

    public boolean isModified() {
        return modified;
    }

    public void setModified(boolean aValue) {
        modified = aValue;
    }

}
