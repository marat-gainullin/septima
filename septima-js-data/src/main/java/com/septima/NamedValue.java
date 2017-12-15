package com.septima;

/**
 * Holds a typed value for some change in a changes log. 
 * @author mg
 */
public class NamedValue {

    private final String name;
    private final Object value;

    public NamedValue(String aName, Object aValue) {
        name = aName;
        value = aValue;
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

}
