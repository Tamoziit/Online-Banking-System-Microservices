package com.tamojit.paymentservice.repository;

import com.tamojit.paymentservice.model.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface PaymentRepository extends JpaRepository<Payment, String> {
    Optional<Payment> findByRazorpayOrderId(String orderId);
}
