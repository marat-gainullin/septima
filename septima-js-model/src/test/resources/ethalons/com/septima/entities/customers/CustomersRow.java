package com.septima.entities.customers;

import com.septima.model.Observable;

public class CustomersRow extends Observable {

    private long id;
    private String name;

    public long getId() {
        return id;
    }

    public void setId(long aId) {
        long old = id;
        id = aId;
        changeSupport.firePropertyChange("id", old, id);
    }

    public String getName() {
        return name;
    }

    public void setName(String aName) {
        String old = name;
        name = aName;
        changeSupport.firePropertyChange("name", old, name);
    }

}
