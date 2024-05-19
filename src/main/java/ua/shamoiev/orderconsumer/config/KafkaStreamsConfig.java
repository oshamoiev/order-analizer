package ua.shamoiev.orderconsumer.config;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import ua.shamoiev.orderconsumer.aggregator.OrderAggregator;
import ua.shamoiev.orderconsumer.dto.OrderDto;

@Configuration
public class KafkaStreamsConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.streams.application-id}")
    private String applicationId;

    @Bean
    Serde<String> getStringSerde() {
        return Serdes.String();
    }

    @Bean
    public Gson gson() {
        return new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
    }

    @Bean
    public Serializer<OrderDto> salesOrderSerializer(Gson gson) {
        return serializer(gson);
    }

    @Bean
    public Serializer<OrderAggregator> salesOrderAnalyserSerializer(Gson gson) {
        return serializer(gson);
    }

    @Bean
    public Deserializer<OrderAggregator> salesOrderAggregatorDeserializer(Gson gson) {
        return deserializer(gson, OrderAggregator.class);
    }

    @Bean
    public Deserializer<OrderDto> orderDeserializer(Gson gson) {
        return deserializer(gson, OrderDto.class);
    }

    @Bean
    public Serde<OrderDto> orderDtoSerde(Serializer<OrderDto> orderDtoSerializer, Deserializer<OrderDto> orderDtoDeserializer) {
        return Serdes.serdeFrom(orderDtoSerializer, orderDtoDeserializer);
    }

    @Bean
    public Serde<OrderAggregator> orderAggregatorSerde(Serializer<OrderAggregator> orderAggregatorSerializer,
            Deserializer<OrderAggregator> orderAggregatorDeserializer) {
        return Serdes.serdeFrom(orderAggregatorSerializer, orderAggregatorDeserializer);
    }

    @Bean
    @Primary
    public Properties getKafkaStreamsProperties(Serde<String> stringSerde) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //For immediate results during testing
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        return props;
    }

    public <T> Serializer<T> serializer(Gson gson) {
        return (topic, type) -> gson.toJson(type).getBytes(StandardCharsets.UTF_8);
    }

    private  <T> Deserializer<T> deserializer(Gson gson, Class<T> destinationClass) {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }

            return gson.fromJson(new String(data), destinationClass);
        };
    }
}
