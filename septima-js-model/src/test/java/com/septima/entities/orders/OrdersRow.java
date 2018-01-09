package com.septima.entities.orders;

import com.septima.model.Observable;
import java.util.Date;

public class OrdersRow extends Observable {

    private String badData;
    private String comment;
    private long customerId;
    private String destination;
    private long goodId;
    private long id;
    private Date moment;
    private Boolean paid;
    private Long sellerId;
    private Double summ;

    public String getBadData() {
        return badData;
    }

    public void setBadData(String aBadData) {
        String old = badData;
        badData = aBadData;
        changeSupport.firePropertyChange("bad_data", old, badData);
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String aComment) {
        String old = comment;
        comment = aComment;
        changeSupport.firePropertyChange("comment", old, comment);
    }

    public long getCustomerId() {
        return customerId;
    }

    public void setCustomerId(long aCustomerId) {
        long old = customerId;
        customerId = aCustomerId;
        changeSupport.firePropertyChange("customer_id", old, customerId);
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String aDestination) {
        String old = destination;
        destination = aDestination;
        changeSupport.firePropertyChange("destination", old, destination);
    }

    public long getGoodId() {
        return goodId;
    }

    public void setGoodId(long aGoodId) {
        long old = goodId;
        goodId = aGoodId;
        changeSupport.firePropertyChange("good_id", old, goodId);
    }

    public long getId() {
        return id;
    }

    public void setId(long aId) {
        long old = id;
        id = aId;
        changeSupport.firePropertyChange("id", old, id);
    }

    public Date getMoment() {
        return moment;
    }

    public void setMoment(Date aMoment) {
        Date old = moment;
        moment = aMoment;
        changeSupport.firePropertyChange("moment", old, moment);
    }

    public Boolean getPaid() {
        return paid;
    }

    public void setPaid(Boolean aPaid) {
        Boolean old = paid;
        paid = aPaid;
        changeSupport.firePropertyChange("paid", old, paid);
    }

    public Long getSellerId() {
        return sellerId;
    }

    public void setSellerId(Long aSellerId) {
        Long old = sellerId;
        sellerId = aSellerId;
        changeSupport.firePropertyChange("seller_id", old, sellerId);
    }

    public Double getSumm() {
        return summ;
    }

    public void setSumm(Double aSumm) {
        Double old = summ;
        summ = aSumm;
        changeSupport.firePropertyChange("summ", old, summ);
    }

}
