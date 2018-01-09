package com.septima;

/**
 * Data types generic because there are many other types in programming languages and in
 * databases. All of them may be grouped into few number og generic groups.
 * This generic types are not mandatory. If some parameter or field has no generic type,
 * then actual type is resolved by value.
 *
 * @author mg
 */
public enum GenericType {

    GEOMETRY,
    STRING,
    DOUBLE,
    LONG,
    DATE,
    BOOLEAN;

    public static GenericType of(String aValue) {
        return of(aValue, null);
    }

    public static GenericType of(String aValue, GenericType defaultValue) {
        if ("Geometry".equalsIgnoreCase(aValue)) {
            return GEOMETRY;
        } else if ("String".equalsIgnoreCase(aValue)) {
            return STRING;
        } else if ("Number".equalsIgnoreCase(aValue)) {
            return DOUBLE;
        } else if ("Double".equalsIgnoreCase(aValue)) {
            return DOUBLE;
        } else if ("Long".equalsIgnoreCase(aValue)) {
            return LONG;
        } else if ("Date".equalsIgnoreCase(aValue)) {
            return DATE;
        } else if ("Boolean".equalsIgnoreCase(aValue)) {
            return BOOLEAN;
        } else {
            return defaultValue;
        }
    }

    public Object narrow(Object aValue) {
        if (aValue != null) {
            switch (this) {
                case DOUBLE:
                    return aValue instanceof Double ? aValue : ((Number) aValue).doubleValue();
                case LONG:
                    return aValue instanceof Long ? aValue : ((Number) aValue).longValue();
                default:
                    return aValue;
            }
        } else {
            return aValue;
        }
    }
}
