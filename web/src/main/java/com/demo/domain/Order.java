package com.demo.domain;

public class Order {
    public String orderId;
    public Long orderTime;
    public String gdsId;
    public Double amount;
    public String areaId;

    public Order() {
    }

    public Order(String orderId, Long orderTime, String gdsId, Double amount, String areaId) {
        this.orderId = orderId;
        this.orderTime = orderTime;
        this.gdsId = gdsId;
        this.amount = amount;
        this.areaId = areaId;
    }
}
