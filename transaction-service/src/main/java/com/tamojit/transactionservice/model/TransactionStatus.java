package com.tamojit.transactionservice.model;

/**
 * Transaction Lifecycle Flow:
 * PENDING -> PROCESSING -> COMPLETED (clean transaction)
 *                       -> PENDING_VERIFICATION (suspicious detected)
 *                              -> COMPLETED (verified) or
 *                              -> FLAGGED (SAGA refund)
 *                       -> FAILED
 *                       -> FLAGGED
 */
public enum TransactionStatus {
    PENDING,
    PROCESSING,
    PENDING_VERIFICATION,
    COMPLETED,
    FAILED,
    FLAGGED
}
