package com.eas.client.dataflow;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO: Refactor this class from constructor to factory.
 * @author mg
 */
public class ColumnsIndicies {

    private final Map<String, Integer> indicies = new HashMap<>();

    public ColumnsIndicies(ResultSetMetaData metaData) throws SQLException {
        super();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String asName = metaData.getColumnLabel(i);
            String name = asName != null && !asName.isEmpty() ? asName : metaData.getColumnName(i);
            indicies.put(name.toLowerCase(), i);
        }
    }

    public int find(String aName) {
        Integer idx = indicies.get(aName.toLowerCase());
        return idx != null ? idx : 0;
    }
}
