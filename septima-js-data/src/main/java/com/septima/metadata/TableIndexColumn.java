package com.septima.metadata;

/**
 * @author mg
 */
public class TableIndexColumn {

    private final String columnName;
    private final boolean ascending;
    private final int ordinalPosition;

    public TableIndexColumn(String aColumnName) {
        this(aColumnName, true);
    }

    public TableIndexColumn(String aColumnName, boolean aAscending) {
        this(aColumnName, aAscending, -1);
    }

    public TableIndexColumn(String aColumnName, boolean aAscending, int aOrdinalPosition) {
        super();
        columnName = aColumnName;
        ascending = aAscending;
        ordinalPosition = aOrdinalPosition;
    }

    public String getColumnName() {
        return columnName;
    }

    public boolean isAscending() {
        return ascending;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }
}
