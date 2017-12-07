package com.septima.dataflow;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mg
 */
public class ColumnsIndicies {


    private ColumnsIndicies() {
    }

    public static Map<String, Integer> of(ResultSetMetaData metaData) throws SQLException {
        final Map<String, Integer> indicies = new HashMap<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String asName = metaData.getColumnLabel(i);
            String name = asName != null && !asName.isEmpty() ? asName : metaData.getColumnName(i);
            indicies.put(name.toLowerCase(), i);
        }
        return Collections.unmodifiableMap(indicies);
    }

}
