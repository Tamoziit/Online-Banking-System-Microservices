package com.tamojit.transactionservice.service;

import com.tamojit.transactionservice.client.AccountServiceClient;
import com.tamojit.transactionservice.dto.TransactionResponse;
import com.tamojit.transactionservice.dto.TransferRequest;
import com.tamojit.transactionservice.event.TransactionInitiatedEvent;
import com.tamojit.transactionservice.model.Transaction;
import com.tamojit.transactionservice.model.TransactionStatus;
import com.tamojit.transactionservice.model.TransactionType;
import com.tamojit.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {
    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;

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
        TransactionResponse transactionResponse = new TransactionResponse();
        return transactionResponse;
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
