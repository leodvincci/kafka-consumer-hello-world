package com.penrose.springkafkaconsumerhelloworld;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class HelloConsumerService {

    private static final Logger log = LoggerFactory.getLogger(HelloConsumerService.class);

    /**
     * Basic listener - just receives the message payload.
     * Good for simple use cases.
     */
    @KafkaListener(topics = "goodnite-world", groupId = "hello-group")
    public void listen(@Payload String message,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                       @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("┌────────────────────────────────────────┐");
        log.info("│ NEW MESSAGE RECEIVED                   │");
        log.info("├────────────────────────────────────────┤");
        log.info("│ Partition: {}                          ", partition);
        log.info("│ Offset:    {}                          ", offset);
        log.info("│ Payload:   {}                          ", message);
        log.info("└────────────────────────────────────────┘");

        // Simulate processing
//        processMessage(message);
    }

    private void processMessage(String message) {
        // This is where your business logic would go
        // For now, we just acknowledge we processed it
        log.info("Successfully processed message: {}", message);
    }

}