package com.septima.model;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;

public abstract class Observable {

    protected final PropertyChangeSupport changeSupport = new PropertyChangeSupport(this);

    public Runnable addListener(PropertyChangeListener listener) {
        changeSupport.addPropertyChangeListener(listener);
        return () -> changeSupport.removePropertyChangeListener(listener);
    }

    public void removeListener(PropertyChangeListener listener) {
        changeSupport.removePropertyChangeListener(listener);
    }

    public Runnable addListener(String aPropertyName, PropertyChangeListener listener) {
        changeSupport.addPropertyChangeListener(aPropertyName, listener);
        return () -> changeSupport.removePropertyChangeListener(aPropertyName, listener);
    }

    public void removeListener(String aPropertyName, PropertyChangeListener listener) {
        changeSupport.removePropertyChangeListener(aPropertyName, listener);
    }

}
