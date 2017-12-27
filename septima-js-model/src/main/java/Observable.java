import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;

public class Observable {
    protected final PropertyChangeSupport changeSupport = new PropertyChangeSupport(this);

    public Runnable addListener(PropertyChangeListener listener) {
        changeSupport.addPropertyChangeListener(listener);
        return () -> {
            changeSupport.removePropertyChangeListener(listener);
        };
    }

    public Runnable addListener(String aPropertyName, PropertyChangeListener listener) {
        changeSupport.addPropertyChangeListener(aPropertyName, listener);
        return () -> {
            changeSupport.removePropertyChangeListener(aPropertyName, listener);
        };
    }
}
