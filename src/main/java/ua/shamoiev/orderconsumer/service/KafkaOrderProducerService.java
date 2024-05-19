package ua.shamoiev.orderconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ua.shamoiev.orderconsumer.dto.OrderDto;

@Service
public class KafkaOrderProducerService {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Value("${spring.kafka.input.topic}")
    private String topicName;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaOrderProducerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaOrderProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrder(OrderDto orderDto) {
        try {
            String value = MAPPER.writeValueAsString(orderDto);
            String recordKey = String.valueOf(orderDto.getOrderId());
            LOGGER.info("Sending Order : {}", orderDto);
            kafkaTemplate.send(topicName, recordKey, value);

        } catch (JsonProcessingException e) {
            LOGGER.error("Json Processing Error while sending order : {}", orderDto, e);
        }
    }
}
