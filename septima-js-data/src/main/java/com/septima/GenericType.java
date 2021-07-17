package com.septima;

import com.fasterxml.jackson.databind.util.StdDateFormat;

import java.text.ParseException;

/**
 * There are many types in programming languages and in
 * databases. All of them may be grouped into few number of generic groups.
 * These generic types are not mandatory. If some parameter or field has no generic type,
 * then actual type is resolved by value.
 *
 * @author mg
 */
public enum GenericType {

    GEOMETRY("Geometry"),
    STRING("String"),
    DOUBLE("Number", "Double"),
    LONG("Long"),
    DATE("Date", "Timestamp"),
    BOOLEAN("Boolean");

    private String text;
    private String alias;

    GenericType(String aText) {
        this(aText, null);
    }

    GenericType(String aText, String aAlias) {
        text = aText;
        alias = aAlias;
    }

    public String getText() {
        return text;
    }

    boolean isName(String aName) {
        return text.equalsIgnoreCase(aName) || alias != null && alias.equalsIgnoreCase(aName);
    }

    public static GenericType of(String aValue) {
        return of(aValue, null);
    }

    public static GenericType of(String aValue, GenericType defaultType) {
        if (GEOMETRY.isName(aValue)) {
            return GEOMETRY;
        } else if (STRING.isName(aValue)) {
            return STRING;
        } else if (DOUBLE.isName(aValue)) {
            return DOUBLE;
        } else if (LONG.isName(aValue)) {
            return LONG;
        } else if (DATE.isName(aValue)) {
            return DATE;
        } else if (BOOLEAN.isName(aValue)) {
            return BOOLEAN;
        } else {
            return defaultType;
        }
    }

    public static Object parseValue(String aValue, GenericType aType) {
        if (aValue != null && !"null".equals(aValue)) {
            switch (aType) {
                case DATE:
                    StdDateFormat format = new StdDateFormat();
                    try {
                        return format.parse(aValue);
                    } catch (ParseException ex) {
                        throw new IllegalStateException(ex);
                    }
                case BOOLEAN:
                    return Boolean.parseBoolean(aValue);
                case DOUBLE:
                    return Double.parseDouble(aValue);
                case LONG:
                    return Long.parseLong(aValue);
                default:
                    return aValue;
            }
        } else {
            return null;
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
            return null;
        }
    }

}
