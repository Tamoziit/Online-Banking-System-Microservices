package com.tamojit.frauddetectionservice.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionEventConsumer {
    private final FraudDetectionService fraudDetectionService;

    /**
     * Listens to transaction.initiated topic
     * Every transaction moves through fraud check before completing
     */
    @KafkaListener(topics = "transaction.initiated", groupId = "fraud-detection-group")
    public void consumeTransactionInitiated(
        @Payload Map<String, Object> payload
    ) {
        log.info("Received transaction for fraud check: {}", payload.get("transactionId"));

        try {
            fraudDetectionService.checkTransaction(payload);
        } catch(Exception e) {}
    }
}
