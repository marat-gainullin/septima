package com.septima.metadata;

import java.util.*;

/**
 * @author mg
 */
public class TableIndex {

    private final String name;
    private final boolean clustered;
    private final boolean hashed;
    private final boolean unique;
    private final Set<Column> columns;

    public TableIndex(String aName) {
        this(aName, false, false, false, Set.of());
    }

    public TableIndex(String aName, boolean aClustered, boolean aHashed, boolean aUnique, Set<Column> aColumns) {
        name = aName;
        clustered = aClustered;
        hashed = aHashed;
        unique = aUnique;
        columns = aColumns;
    }

    public Set<Column> getColumns() {
        return columns;
    }

    public boolean isUnique() {
        return unique;
    }

    public boolean isClustered() {
        return clustered;
    }

    public boolean isHashed() {
        return hashed;
    }

    public String getName() {
        return name;
    }

    /**
     * @author mg
     */
    public static class Column {

        private final String columnName;
        private final boolean ascending;
        private final int ordinalPosition;

        public Column(String aColumnName) {
            this(aColumnName, true);
        }

        public Column(String aColumnName, boolean aAscending) {
            this(aColumnName, aAscending, -1);
        }

        public Column(String aColumnName, boolean aAscending, int aOrdinalPosition) {
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
}
