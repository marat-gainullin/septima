package com.septima.metadata;

/**
 * @author mg
 */
public class TableIndexColumn {

    protected String columnName;
    protected boolean ascending = true;
    protected int ordinalPosition = -1;

    public TableIndexColumn(String aColumnName, boolean aAscending) {
        super();
        columnName = aColumnName;
        ascending = aAscending;
    }

    public TableIndexColumn(TableIndexColumn aSource) {
        columnName = new String(aSource.getColumnName().toCharArray());
        ascending = aSource.isAscending();
        ordinalPosition = aSource.getOrdinalPosition();
    }

    public TableIndexColumn copy() {
        return new TableIndexColumn(this);
    }

    public String getColumnName() {
        return columnName;
    }

    public boolean isAscending() {
        return ascending;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void setAscending(boolean anAscending) {
        ascending = anAscending;
    }

    public void setOrdinalPosition(int anOrdinalPosition) {
        ordinalPosition = anOrdinalPosition;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }
}
