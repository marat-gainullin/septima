/*
 * This source file is generated automatically.
 * Please, don't edit it manually.
 */
package com.septima.entities.orders;

import com.septima.model.Observable;

import java.util.Date;

public class OrdersRow extends Observable {

    private static final long serialVersionUID = 1L;

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

    public void setBadData(String aValue) {
        String old = badData;
        badData = aValue;
        changeSupport.firePropertyChange("bad_data", old, badData);
    }
    public String getComment() {
        return comment;
    }

    public void setComment(String aValue) {
        String old = comment;
        comment = aValue;
        changeSupport.firePropertyChange("comment", old, comment);
    }
    public long getCustomerId() {
        return customerId;
    }

    public void setCustomerId(long aValue) {
        long old = customerId;
        customerId = aValue;
        changeSupport.firePropertyChange("customer_id", old, customerId);
    }
    public String getDestination() {
        return destination;
    }

    public void setDestination(String aValue) {
        String old = destination;
        destination = aValue;
        changeSupport.firePropertyChange("destination", old, destination);
    }
    public long getGoodId() {
        return goodId;
    }

    public void setGoodId(long aValue) {
        long old = goodId;
        goodId = aValue;
        changeSupport.firePropertyChange("good_id", old, goodId);
    }
    public long getId() {
        return id;
    }

    public void setId(long aValue) {
        long old = id;
        id = aValue;
        changeSupport.firePropertyChange("id", old, id);
    }
    public Date getMoment() {
        return moment;
    }

    public void setMoment(Date aValue) {
        Date old = moment;
        moment = aValue;
        changeSupport.firePropertyChange("moment", old, moment);
    }
    public Boolean getPaid() {
        return paid;
    }

    public void setPaid(Boolean aValue) {
        Boolean old = paid;
        paid = aValue;
        changeSupport.firePropertyChange("paid", old, paid);
    }
    public Long getSellerId() {
        return sellerId;
    }

    public void setSellerId(Long aValue) {
        Long old = sellerId;
        sellerId = aValue;
        changeSupport.firePropertyChange("seller_id", old, sellerId);
    }
    public Double getSumm() {
        return summ;
    }

    public void setSumm(Double aValue) {
        Double old = summ;
        summ = aValue;
        changeSupport.firePropertyChange("summ", old, summ);
    }

}
