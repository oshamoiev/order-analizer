package ua.shamoiev.orderconsumer.ordergenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.springframework.stereotype.Component;
import ua.shamoiev.orderconsumer.dto.OrderDto;

@Component
public class OrderGenerator {

    private static final List<String> products = new ArrayList<>();
    private static final List<Double> prices = new ArrayList<>();
    public static final int MAX_ITEM_COUNT = 4;
    public static final Random RANDOM = new Random();

    static {
        products.add("Keyboard");
        products.add("Mouse");
        products.add("Monitor");

        prices.add(10.00);
        prices.add(25.00);
        prices.add(50.00);
        prices.add(140.00);
    }

    public OrderDto generateOrder() {

        int orderId = (int) (System.currentTimeMillis() / 1000);

        OrderDto orderDto = new OrderDto();
        orderDto.setOrderId(orderId);

        int product = RANDOM.nextInt(products.size());
        orderDto.setProduct(products.get(product));

        int price = RANDOM.nextInt(prices.size());
        orderDto.setPrice(prices.get(price));

        orderDto.setQuantity(RANDOM.nextInt(MAX_ITEM_COUNT) + 1);

        return orderDto;
    }
}
