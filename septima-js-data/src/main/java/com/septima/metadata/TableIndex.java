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
    private final Set<TableIndexColumn> columns;

    public TableIndex(String aName) {
        this(aName, false, false, false, Set.of());
    }

    public TableIndex(String aName, boolean aClustered, boolean aHashed, boolean aUnique, Set<TableIndexColumn> aColumns) {
        name = aName;
        clustered = aClustered;
        hashed = aHashed;
        unique = aUnique;
        columns = aColumns;
    }

    public Set<TableIndexColumn> getColumns() {
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

}
