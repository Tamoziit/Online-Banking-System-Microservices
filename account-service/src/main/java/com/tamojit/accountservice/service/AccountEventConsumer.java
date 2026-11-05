package com.tamojit.accountservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {
    private final AccountService accountService;

    /**
     * Consumes transaction.completed event from Kafka
     * Credits receiver account
     */
    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(
        @Payload Map<String, Object> payload
    ) {
        try {
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());
            log.info("Crediting acc. {} with amt: {}", receiverAccount, amount);

            accountService.creditBalance(receiverAccount, amount);
        } catch (Exception e) {
            log.error("Error while crediting account: {}", e.getMessage());
        }
    }

    /**
     * Consumes fraud.detected event from Kafka
     * Blocks the flagged account
     */
    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(
        @Payload Map<String, Object> payload
    ) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            log.info("Fraud detected - Blocking acc. no.: {}", accountNumber);

            accountService.blockAccount(accountNumber);
        } catch (Exception e) {
            log.error("Error while blocking account: {}", e.getMessage());
        }
    }
}
