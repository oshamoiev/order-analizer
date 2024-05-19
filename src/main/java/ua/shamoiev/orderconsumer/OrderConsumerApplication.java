package ua.shamoiev.orderconsumer;

import java.util.Random;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;
import ua.shamoiev.orderconsumer.analyzing.OrderAnalyser;
import ua.shamoiev.orderconsumer.dto.OrderDto;
import ua.shamoiev.orderconsumer.ordergenerator.OrderGenerator;
import ua.shamoiev.orderconsumer.service.KafkaOrderProducerService;

@SpringBootApplication
public class OrderConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderConsumerApplication.class, args);
    }

    @Component
    public static class OrderPublishingCommandLineRunner implements CommandLineRunner {

        final OrderGenerator orderGenerator;
        final KafkaOrderProducerService kafkaOrderProducerService;
        final Random RANDOM = new Random();


        public OrderPublishingCommandLineRunner(OrderGenerator orderGenerator,
                KafkaOrderProducerService kafkaOrderProducerService) {
            this.orderGenerator = orderGenerator;
            this.kafkaOrderProducerService = kafkaOrderProducerService;
        }

        @Override
        public void run(String... args) throws Exception {
            new Thread(() -> {
                try {
                    Thread.sleep(5000);


                    while (true) {
                        long intervalBetwenOrderGeneration = RANDOM.nextInt(3);
                        Thread.sleep(intervalBetwenOrderGeneration * 1000);
                        OrderDto orderDto = orderGenerator.generateOrder();
                        kafkaOrderProducerService.sendOrder(orderDto);
                    }
                } catch (InterruptedException e) {
                    // do nothing
                }
            }).start();
        }
    }

    @Component
    public static class OrderAgregatorCommandLineRunner implements CommandLineRunner {

        private final OrderAnalyser orderAnalyser;

        public OrderAgregatorCommandLineRunner(OrderAnalyser orderAnalyser) {
            this.orderAnalyser = orderAnalyser;
        }

        @Override
        public void run(String... args) {
            orderAnalyser.aggregateOrders();
        }
    }
}
