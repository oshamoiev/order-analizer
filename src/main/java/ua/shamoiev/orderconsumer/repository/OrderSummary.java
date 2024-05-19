package ua.shamoiev.orderconsumer.repository;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

@Entity
public class OrderSummary {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String intervalTimestamp;

    private String product;

    private Double totalValue;

    public OrderSummary(String intervalTimestamp, String product, Double totalValue) {
        this.intervalTimestamp = intervalTimestamp;
        this.product = product;
        this.totalValue = totalValue;
    }

    public OrderSummary() {
    }

    public String getIntervalTimestamp() {
        return intervalTimestamp;
    }

    public void setIntervalTimestamp(String intervalTimestamp) {
        this.intervalTimestamp = intervalTimestamp;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public Double getTotalValue() {
        return totalValue;
    }

    public void setTotalValue(Double totalValue) {
        this.totalValue = totalValue;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }
}
