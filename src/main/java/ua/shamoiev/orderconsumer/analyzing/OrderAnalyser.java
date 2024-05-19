package ua.shamoiev.orderconsumer.analyzing;

import java.time.Duration;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ua.shamoiev.orderconsumer.aggregator.OrderAggregator;
import ua.shamoiev.orderconsumer.dto.OrderDto;
import ua.shamoiev.orderconsumer.repository.OrderSummary;
import ua.shamoiev.orderconsumer.repository.OrderSummaryRepository;

@Component
public class OrderAnalyser {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderAnalyser.class);
    private static final int TIME_WINDOW_SIZE_SEC = 10;

    private final Serde<String> stringSerde;
    private final Serde<OrderDto> orderDtoSerde;
    private final Serde<OrderAggregator> orderAggregatorSerde;

    private final Properties kafkaStreamsProperties;

    private final OrderSummaryRepository orderSummaryRepository;

    @Value("${spring.kafka.input.topic}")
    private String inputTopic;

    public OrderAnalyser(Serde<String> stringSerde, Serde<OrderDto> orderDtoSerde,
            Serde<OrderAggregator> orderAggregatorSerde, Properties kafkaStreamsProperties,
            OrderSummaryRepository orderSummaryRepository) {
        this.stringSerde = stringSerde;
        this.orderDtoSerde = orderDtoSerde;
        this.orderAggregatorSerde = orderAggregatorSerde;
        this.kafkaStreamsProperties = kafkaStreamsProperties;
        this.orderSummaryRepository = orderSummaryRepository;
    }

    public void aggregateOrders() {

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> orderJsonInputStream = getOrderJsonInputStream(builder);

        KStream<String, OrderDto> salesOrderDtoKStream = convertToOrderDtoStream(orderJsonInputStream);
        salesOrderDtoKStream.peek((key, value) -> LOGGER.info("Received Order : {}", value));

        KTable<Windowed<String>, OrderAggregator> windowedProductSummary = getWindowedProductSummary(salesOrderDtoKStream);

        printAndSaveToDB(windowedProductSummary);

        final Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, kafkaStreamsProperties);
        streams.start();
    }

    private KStream<String, String> getOrderJsonInputStream(StreamsBuilder streamsBuilder) {
        return streamsBuilder.stream(inputTopic, Consumed.with(stringSerde, stringSerde));
    }

    private static KStream<String, OrderDto> convertToOrderDtoStream(KStream<String, String> ordersInput) {
        ObjectMapper mapper = new ObjectMapper();

        return ordersInput.mapValues(
                inputJson -> {
                    try {
                        return mapper.readValue(inputJson, OrderDto.class);
                    } catch (Exception e) {
                        LOGGER.error("ERROR : Cannot convert JSON {}", inputJson);
                        return null;
                    }
                }
        );
    }

    private KTable<Windowed<String>, OrderAggregator> getWindowedProductSummary(KStream<String, OrderDto> salesOrderDtoKStream) {
        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(TIME_WINDOW_SIZE_SEC));

        Initializer<OrderAggregator> orderAggregatorInitializer = OrderAggregator::new;

        Aggregator<String, OrderDto, OrderAggregator> orderAdder = (key, value, aggregate)
                -> aggregate.add(value.getPrice() * value.getQuantity());

        return salesOrderDtoKStream
                .groupBy((key, value) -> value.getProduct(),
                        Grouped.with(stringSerde, orderDtoSerde))
                .windowedBy(tumblingWindow)
                .aggregate(orderAggregatorInitializer, orderAdder,
                        Materialized.<String, OrderAggregator, WindowStore<Bytes, byte[]>>as(
                                        "time-windowed-aggregate-store")
                                .withValueSerde(orderAggregatorSerde))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));
    }

    private void printAndSaveToDB(KTable<Windowed<String>, OrderAggregator> windowedProductSummary) {
        windowedProductSummary.toStream()
                .foreach((key, aggregation) -> {
                            LOGGER.info("Received Summary : Window = {} Product = {} Value = {}",
                                    key.window().startTime(), key.key(), aggregation.getTotalValue());

                            orderSummaryRepository.save(new OrderSummary(key.window().startTime().toString(),
                                    key.key(), aggregation.getTotalValue()
                            ));
                        }
                );
    }
}
