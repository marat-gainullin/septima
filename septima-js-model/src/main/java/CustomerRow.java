
public class CustomerRow extends Observable {
    private long id;
    private String name;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        long old = id;
        this.id = id;
        changeSupport.firePropertyChange("id", old, id);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        String old = name;
        this.name = name;
        changeSupport.firePropertyChange("name", old, name);
    }

}
