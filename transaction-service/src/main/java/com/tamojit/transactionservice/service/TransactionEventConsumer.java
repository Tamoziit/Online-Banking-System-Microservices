package com.tamojit.transactionservice.service;

import com.tamojit.transactionservice.model.Transaction;
import com.tamojit.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionEventConsumer {
    private final TransactionRepository transactionRepository;

    /**
     * Consumes verification.required event, published by fraud-detection-service
     * Next steps: Generate OTP & ask user to verify for authenticity check
     */
    public void consumeVerificationRequiredEvent(
        @Payload Map<String, Object> payload
    ) {
        try {
            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("accountNumber");
            String reason =  (String) payload.get("reason");

            log.info("Verification required - transaction: {}, account: {}, reason: {}",  transactionId, accountNumber, reason);

            Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("transaction not found: " +  transactionId));

            
        } catch (Exception e) {

        }
    }
}
