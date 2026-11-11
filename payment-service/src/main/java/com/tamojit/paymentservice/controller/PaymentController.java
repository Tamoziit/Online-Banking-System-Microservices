package com.tamojit.paymentservice.controller;

import com.razorpay.RazorpayException;
import com.tamojit.paymentservice.dto.CreatePaymentRequest;
import com.tamojit.paymentservice.dto.PaymentOrderResponse;
import com.tamojit.paymentservice.service.PaymentService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/payments")
@Slf4j
@RequiredArgsConstructor
public class PaymentController {
    private final PaymentService paymentService;

    @PostMapping("/create-order")
    public ResponseEntity<PaymentOrderResponse> createPaymentOrder(
        @Valid @RequestBody CreatePaymentRequest request
    ) throws RazorpayException {
        return ResponseEntity.status(HttpStatus.CREATED)
            .body(paymentService.createPaymentOrder(request));
    }

    // to handle razorpay webhook for payment verification
    @PostMapping("/webhook")
    public ResponseEntity<String> handleWebhook(
        @RequestBody Map<String, Object> payload
    ) {
        paymentService.handleWebhook(payload);
        return ResponseEntity.ok("Webhook processed successfully");
    }
}
