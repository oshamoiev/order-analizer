package ua.shamoiev.orderconsumer.aggregator;


public class OrderAggregator {

    private Double totalValue = 0.0;

    public Double getTotalValue() {
        return totalValue;
    }

    public OrderAggregator add(Double value) {
        totalValue += value;
        return this;
    }
}
