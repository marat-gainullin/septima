package com.septima.metadata;

import com.septima.GenericType;

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
    private final String description;

    private final GenericType type;
    private final Mode mode;
    private Object value;

    public Parameter(String aName) {
        this(aName, null);
    }

    public Parameter(String aName, Object aValue) {
        this(aName, aValue, null);
    }

    public Parameter(String aName, Object aValue, GenericType aType) {
        this(aName, aValue, aType, Mode.In);
    }

    public Parameter(String aName, Object aValue, GenericType aType, Mode aMode) {
        this(aName, aValue, aType, aMode, null);
    }

    public Parameter(String aName, Object aValue, GenericType aType, Mode aMode, String aDescription) {
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

    public GenericType getType() {
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
}
