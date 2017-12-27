public class OrderRow extends Observable {
    private long id;
    private Long customer_id;
    private Long good_id;
    private String comment;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        long old = id;
        this.id = id;
        changeSupport.firePropertyChange("id", old, id);
    }

    public Long getCustomerId() {
        return customer_id;
    }

    public void setCustomerId(Long customer_id) {
        Long old = customer_id;
        this.customer_id = customer_id;
        changeSupport.firePropertyChange("customer_id", old, customer_id);
    }

    public Long getGoodId() {
        return good_id;
    }

    public void setGoodId(Long good_id) {
        Long old = good_id;
        this.good_id = good_id;
        changeSupport.firePropertyChange("good_id", old, good_id);
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        String old = comment;
        this.comment = comment;
        changeSupport.firePropertyChange("comment", old, comment);
    }

}
