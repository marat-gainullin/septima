package com.septima.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author mg
 */
public class TableIndex {

    protected List<TableIndexColumn> columns = new ArrayList<>();
    protected boolean clustered;
    protected boolean hashed;
    protected boolean unique;
    protected String name;

    public TableIndex() {
        super();
    }

    public TableIndex(String aName) {
        super();
        name = aName;
    }

    public TableIndex(TableIndex aSource) {
        super();
        assert aSource != null;
        clustered = aSource.isClustered();
        hashed = aSource.isHashed();
        unique = aSource.isUnique();
        name = null;
        if (aSource.getName() != null) {
            name = new String(aSource.getName().toCharArray());
        }
        List<TableIndexColumn> sourceColumns = aSource.getColumns();
        for (int i = 0; i < sourceColumns.size(); i++) {
            columns.add(sourceColumns.get(i).copy());
        }
    }

    public List<TableIndexColumn> getColumns() {
        return columns;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean aValue) {
        unique = aValue;
    }

    public boolean isClustered() {
        return clustered;
    }

    public void setClustered(boolean aValue) {
        clustered = aValue;
    }

    public boolean isHashed() {
        return hashed;
    }

    public void setHashed(boolean aValue) {
        hashed = aValue;
    }

    public String getName() {
        return name;
    }

    public void setName(String aValue) {
        name = aValue;
    }

    public TableIndexColumn getColumn(String aColumnName) {
        for (int i = 0; i < columns.size(); i++) {
            TableIndexColumn column = columns.get(i);
            if (column.getColumnName().equalsIgnoreCase(aColumnName)) {
                return column;
            }
        }
        return null;
    }

    public void addColumn(TableIndexColumn aColumn) {
        if (aColumn != null && getColumn(aColumn.getColumnName()) == null) {
            columns.add(aColumn);
        }
    }

    public boolean findColumnByName(String aColumnName)
    {
        return indexOfColumnByName(aColumnName) != -1;
    }

    public int indexOfColumnByName(String aColumnName)
    {
        if(aColumnName != null && !aColumnName.isEmpty())
        {
            for(int i=0;i<columns.size();i++)
            {
                TableIndexColumn column = columns.get(i);
                if(aColumnName.equalsIgnoreCase(column.getColumnName()))
                    return i;
            }
        }
        return -1;
    }

    public void sortColumns() {
        Map<Integer, TableIndexColumn> tm = new TreeMap<>();
        for (TableIndexColumn column : columns) {
            tm.put(column.getOrdinalPosition(), column);
        }
        columns.clear();
        for (TableIndexColumn column : tm.values()) {
            columns.add(column);
        }
    }
}
