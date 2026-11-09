package com.tamojit.transactionservice.service;

import com.tamojit.transactionservice.client.AccountServiceClient;
import com.tamojit.transactionservice.dto.TransactionResponse;
import com.tamojit.transactionservice.dto.TransferRequest;
import com.tamojit.transactionservice.event.TransactionCompletedEvent;
import com.tamojit.transactionservice.event.TransactionInitiatedEvent;
import com.tamojit.transactionservice.model.Transaction;
import com.tamojit.transactionservice.model.TransactionStatus;
import com.tamojit.transactionservice.model.TransactionType;
import com.tamojit.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {
    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    /**
     * SAGA Step 1: Initiate transfer
     * Deducts balance from sender via Feign Client
     * Saves transaction as PROCESSING
     * SAGA Step 2: Publishes transaction.initiated event to Kafka for fraud-detection-service
     */
    public TransactionResponse transfer(TransferRequest request) {
        log.info("SAGA Start > transfer from {} -> {}: amt. = {}", request.getSenderAccountNumber(), request.getReceiverAccountNumber(), request.getAmount());

        // SAGA Step 1: Deducting from sender
        accountServiceClient.deductBalance(
            request.getSenderAccountNumber(),
            request.getAmount()
        );

        // saving Transaction as PROCESSING
        Transaction transaction = new Transaction();
        transaction.setSenderAccountNumber(request.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(request.getReceiverAccountNumber());
        transaction.setAmount(request.getAmount());
        transaction.setType(TransactionType.TRANSFER);
        transaction.setStatus(TransactionStatus.PROCESSING);
        transaction.setDescription(request.getDescription());
        transaction.setReferenceNumber(UUID.randomUUID().toString());

        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("Transaction saved as PROCESSING: {}", savedTransaction.getId());

        // SAGA Step 2: Publishing transaction.initiated event to Kafka for fraud check
        TransactionInitiatedEvent event = new TransactionInitiatedEvent(
            savedTransaction.getId(),
            savedTransaction.getSenderAccountNumber(),
            savedTransaction.getReceiverAccountNumber(),
            savedTransaction.getAmount(),
            savedTransaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC, savedTransaction.getId(), event);
        log.info("SAGA Step 2: transaction.initiated event published for: {}", event.getTransactionId());

        return mapToResponse(savedTransaction);
    }

    public TransactionResponse getTransaction(String transactionId) {
        return mapToResponse(transactionRepository
            .findById(transactionId)
            .orElseThrow(() -> new RuntimeException("Transaction not found: " + transactionId))
        );
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
        return transactionRepository.findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
            .stream()
            .map(this::mapToResponse)
            .collect(Collectors.toList());
    }

    public TransactionResponse verifyOTP(String transactionId, String otp) {
        log.info("OTP verification for transactionId={}, otp={}", transactionId, otp);

        Transaction transaction = transactionRepository.findById(transactionId)
            .orElseThrow(() -> new RuntimeException("Transaction not found: " + transactionId));

        String otpKey = "verification:otp" + transactionId;
        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if (storedOtp == null) {
            // OTP expired
            log.warn("OTP expired for transaction: {}", transactionId);
            compensateTransaction(transaction, "OTP expired - transaction cancelled & amount refunded");

            return mapToResponse(transaction);
        }

        if (!storedOtp.equals(otp)) {
            // Wrong OTP - Block acc. & refund
            log.warn("Wrong OTP - blocking account & refunding: {}", transactionId);
            redisTemplate.delete(otpKey);
            blockAccountAndCompensate(transaction, "Wrong OTP entered - transaction cancelled & account blocked for security");

            return mapToResponse(transaction);
        }

        // OTP correct - complete transaction
        log.info("OTP verified, completing transaction: {}", transactionId);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);

        return mapToResponse(transaction);
    }

    private void compensateTransaction(Transaction transaction, String reason) {
        log.warn("SAGA Compensation - refunding account: {}, amount: {}", transaction.getSenderAccountNumber(), transaction.getAmount());

        // Credit money back to sender synchronously
        accountServiceClient.creditBalance(
            transaction.getSenderAccountNumber(),
            transaction.getAmount()
        );

        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason + " - SAGA Compensation executed, amount refunded at " + LocalDateTime.now());
        transactionRepository.save(transaction);

        // Publishing refund event for notification-service to alert user
        Map<String, Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId", transaction.getId());
        refundEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("amount", transaction.getAmount());
        refundEvent.put("reason", reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC, transaction.getId(), refundEvent);
        log.info("SAGA Compensation complete - {} refunded to {}", transaction.getAmount(), transaction.getSenderAccountNumber());
    }

    private void blockAccountAndCompensate(Transaction transaction, String reason) {
        // Publishing fraud.detected event - account-service will block the account
        Map<String, Object> fraudEvent = new HashMap<>();
        fraudEvent.put("transactionId", transaction.getId());
        fraudEvent.put("accountNumber", transaction.getSenderAccountNumber());
        fraudEvent.put("reason", reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC, transaction.getId(), fraudEvent);
        log.warn("fraud.detected published - account: {} will be blocked", transaction.getSenderAccountNumber());

        // SAGA Compensation - refund sender
        compensateTransaction(transaction, reason);
    }

    private void completeTransaction(Transaction transaction) {
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        // publishing transaction.completed event
        TransactionCompletedEvent transactionCompletedEvent = new TransactionCompletedEvent(
            transaction.getId(),
            transaction.getSenderAccountNumber(),
            transaction.getReceiverAccountNumber(),
            transaction.getAmount(),
            transaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(), transactionCompletedEvent);
        log.info("SAGA Completed for transaction: {}", transaction.getId());
    }

    private TransactionResponse mapToResponse(Transaction transaction) {
        TransactionResponse response = new TransactionResponse();

        response.setId(transaction.getId());
        response.setSenderAccountNumber(transaction.getSenderAccountNumber());
        response.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
        response.setAmount(transaction.getAmount());
        response.setType(transaction.getType());
        response.setStatus(transaction.getStatus());
        response.setDescription(transaction.getDescription());
        response.setReferenceNumber(transaction.getReferenceNumber());
        response.setFailureReason(transaction.getFailureReason());
        response.setCreatedAt(transaction.getCreatedAt());
        response.setCompletedAt(transaction.getCompletedAt());

        return response;
    }
}
