package com.tamojit.transactionservice.service;

import com.tamojit.transactionservice.model.Transaction;
import com.tamojit.transactionservice.model.TransactionStatus;
import com.tamojit.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionEventConsumer {
    private static final long OTP_EXPIRY_MINUTES = 5; // OTP expires in 5 mins.
    private static final String TRANSACTION_OTP_GENERATED_TOPIC = "transaction.otp.generated";

    private final TransactionRepository transactionRepository;
    private final RedisTemplate<String, String> redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Consumes verification.required event, published by fraud-detection-service
     * Next steps: Generate OTP & ask user to verify for authenticity check
     */
    @KafkaListener(topics = "verification.required")
    public void consumeVerificationRequiredEvent(
        @Payload Map<String, Object> payload
    ) {
        try {
            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            log.info("Verification required - transaction: {}, account: {}, reason: {}", transactionId, accountNumber, reason);

            Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("transaction not found: " + transactionId));

            // IDEMPOTENCY GUARD
            if (transaction.getStatus() != TransactionStatus.PROCESSING) {
                log.warn("Transaction {} not PROCESSING - skipping verification", transactionId);
                return;
            }

            // Generating 6-digit OTP
            String otp = String.format("%06d", (int) (Math.random() * 900000) + 100000);

            // Storing OTP in redis
            String otpKey = "verification:otp" + transactionId;
            redisTemplate.opsForValue().set(otpKey, otp, OTP_EXPIRY_MINUTES, TimeUnit.MINUTES);

            // Updating status to PENDING_VERIFICATION
            transaction.setStatus(TransactionStatus.PENDING_VERIFICATION);
            transactionRepository.save(transaction);

            log.info("OTP generated for transaction: {}", transactionId);

            // Notify user - publish "transaction.otp.generated" event for notification-service
            Map<String, Object> otpEvent = new HashMap<>();
            otpEvent.put("transactionId", transactionId);
            otpEvent.put("accountNumber", accountNumber);
            otpEvent.put("reason", reason);
            otpEvent.put("otp", otp);
            otpEvent.put("amount", payload.get("amount"));

            kafkaTemplate.send(TRANSACTION_OTP_GENERATED_TOPIC, transactionId, otpEvent);
        } catch (Exception e) {
            log.error("Error consuming verification required event: ", e);
        }
    }
}
