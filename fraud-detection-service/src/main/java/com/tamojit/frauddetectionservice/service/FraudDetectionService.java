package com.tamojit.frauddetectionservice.service;

import com.tamojit.frauddetectionservice.client.AccountServiceClient;
import com.tamojit.frauddetectionservice.model.FraudCheckResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionService {
    private static final String VERIFICATION_REQUIRED_TOPIC = "verification.required";
    private static final String FRAUD_CHECK_CLEAN_TOPIC = "fraud.check.clean";

    @Value("${fraud.max-transactions-per-minute")
    private int maxTransactionsPerMinute;

    @Value("${fraud.suspicious-amount-multiplier}")
    private double suspiciousAmountMultiplier;

    @Value("${fraud.max-balance-percentage}")
    private double maxBalancePercentage;

    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    public void checkTransaction(Map<String, Object> payload) {
        String transactionId = (String) payload.get("transactionId");
        String accountNumber = (String) payload.get("senderAccountNumber");
        BigDecimal amount = new BigDecimal(payload.get("amount").toString());

        // Fetching real balance from account-service
        BigDecimal senderBalance = accountServiceClient.getBalance(accountNumber);

        log.info("Checking transaction: {}, account: {}, amount: {}, balance: {}", transactionId, accountNumber, amount, senderBalance);

        FraudCheckResult result = performFraudChecks(
            accountNumber,
            amount,
            senderBalance
        );

        // Fraud detected - sending verification.required event for OTP verification
        if (result.isFraud()) {
            log.info("Suspicious activity detected - account: {}, reason: {}. Requesting OTP Verification", accountNumber, result.getReason());

            Map<String, Object> verificationEvent = new HashMap<>();
            verificationEvent.put("transactionId", transactionId);
            verificationEvent.put("accountNumber", accountNumber);
            verificationEvent.put("amount", amount);
            verificationEvent.put("reason", result.getReason());

            kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC, transactionId, verificationEvent);
        } else { // No fraud detected - pass transaction with fraud.check.clean event
            log.info("Transaction: {} clean - no fraud detected", transactionId);

            Map<String, Object> transactionCleanEvent = new HashMap<>();
            transactionCleanEvent.put("transactionId", transactionId);
            transactionCleanEvent.put("isFraud", false);
            transactionCleanEvent.put("reason", null);

            kafkaTemplate.send(FRAUD_CHECK_CLEAN_TOPIC, transactionId, transactionCleanEvent);
        }
    }

    private FraudCheckResult performFraudChecks(
        String accountNumber,
        BigDecimal amount,
        BigDecimal senderBalance
    ) {
        // Pattern 1: Velocity Check
        if (isVelocityExceeded(accountNumber)) {
            return new FraudCheckResult(
                true,
                "Too many transactions in 60 sec. - Velocity limit exceeded"
            );
        }

        // Pattern 2: Amount Check
        if (isAmountSuspicious(accountNumber, amount)) {
            return new FraudCheckResult(
                true,
                "Unusual transaction amount - exceeds 3x your average transaction amounts"
            );
        }

        // Pattern 3: Balance Check
        if (senderBalance.compareTo(BigDecimal.ZERO) > 0 && isBalanceCheckFailed(senderBalance, amount)) {
            return new FraudCheckResult(
                true,
                "Transaction exceeds 90% of account balance"
            );
        }

        // Clean transaction - No Fraud
        return new FraudCheckResult(
            false,
            null
        );
    }

    // Velocity Check: checking if there were > 5 transactions in 1 min. (60 sec)
    private boolean isVelocityExceeded(String accountNumber) {
        String key = "fraud:velocity" + accountNumber;
        Long count = redisTemplate.opsForValue().increment(key); // increment key on each transaction

        // setting a expiry time of 60 sec on 1st transaction
        if (count != null && count == 1) {
            redisTemplate.expire(key, 60, TimeUnit.SECONDS);
        }

        log.info("Velocity check - account: {}, count: {}/{}", accountNumber, count, maxTransactionsPerMinute);

        return count != null && count > maxTransactionsPerMinute;
    }

    // Amount Check: Amt. > 5x avg transaction amt.
    private boolean isAmountSuspicious(String accountNumber, BigDecimal amount) {
        String avgKey = "fraud:avg_amount" + accountNumber;
        String avgStr = redisTemplate.opsForValue().get(avgKey);

        // 1st transaction
        if (avgStr == null) {
            redisTemplate.opsForValue().set(avgKey, amount.toString());
            return false;
        }

        BigDecimal avgAmount = new BigDecimal(avgStr);
        BigDecimal threshold = avgAmount.multiply(
            BigDecimal.valueOf(suspiciousAmountMultiplier)
        ); // 5x avg

        // updating running avg
        BigDecimal newAvg = avgAmount.add(amount)
            .divide(BigDecimal.valueOf(2), 2, RoundingMode.HALF_UP);

        redisTemplate.opsForValue().set(avgKey, newAvg.toString());

        boolean isSuspicious = amount.compareTo(threshold) > 0;
        log.info("Amount check - amount: {}, threshold: {}, isSuspicious: {}", amount, threshold, isSuspicious);

        return isSuspicious;
    }

    // Balance Check: Amt. > 90% of Balance
    private boolean isBalanceCheckFailed(BigDecimal senderBalance, BigDecimal amount) {
        BigDecimal maxAllowed = senderBalance.multiply(
            BigDecimal.valueOf(maxBalancePercentage)
        );

        boolean isSuspicious = amount.compareTo(maxAllowed) > 0;
        log.info("Balance check - amount: {}, maxAllowed: {}, isSuspicious: {}", amount, maxAllowed, isSuspicious);

        return isSuspicious;
    }
}
