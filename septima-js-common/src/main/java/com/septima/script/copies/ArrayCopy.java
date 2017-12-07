package com.septima.script.copies;

import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 *
 * @author mg
 */
public class ArrayCopy extends LinkedList<Object> {

    // TODO: check equals / hashCode contract

    protected Map<String, Object> props = new TreeMap<>();

    public Object get(String k) {
        return props.get(k);
    }

    public Object put(String k, Object v) {
        return props.put(k, v);
    }

    public Set<String> keySet() {
        return props.keySet();
    }
}
