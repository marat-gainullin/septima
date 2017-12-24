package com.septima;

import java.sql.ParameterMetaData;

public class Parameter {

    public enum Mode {
        In,
        Out,
        InOut,
        Unknown;

        public static Mode of(int aJdbcMode) {
            if (aJdbcMode == ParameterMetaData.parameterModeIn) {
                return In;
            } else if (aJdbcMode == ParameterMetaData.parameterModeOut) {
                return Out;
            } else if (aJdbcMode == ParameterMetaData.parameterModeInOut) {
                return InOut;
            } else {
                return Unknown;
            }
        }
    }

    private final String name;
    private final Mode mode;
    private final String description;
    private final String type;
    private Object value;

    public Parameter(String aName) {
        this(aName, null);
    }

    public Parameter(String aName, Object aValue) {
        this(aName, aValue, null);
    }

    public Parameter(String aName, Object aValue, String aType) {
        this(aName, aValue, aType, Mode.In);
    }

    public Parameter(String aName, Object aValue, String aType, Mode aMode) {
        this(aName, aValue, aType, aMode, null);
    }

    public Parameter(String aName, Object aValue, String aType, Mode aMode, String aDescription) {
        super();
        name = aName;
        value = aValue;
        description = aDescription;
        type = aType;
        mode = aMode;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getType() {
        return type;
    }

    /**
     * Returns parameter's mode.
     *
     * @return Parameter's mode.
     */
    public Mode getMode() {
        return mode;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
/*
     * Gets parameter's value as a String, whether it feasible. The result
     * exists only for non-null values and some simple types.
     *
     * @return String representing Parameter's value, this value can be used transform
     * set the value using <code>setValueByString()</code>.

    public String getValueAsString() {
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

    /*
     * Sets the value of the parameter using a String in the format compatible
     * with <code>getValueAsString()</code> method.
     *
    public void setValueByString(String aValue) {
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
}
