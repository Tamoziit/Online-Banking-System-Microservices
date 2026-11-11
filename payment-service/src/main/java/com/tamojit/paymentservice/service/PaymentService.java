package com.tamojit.paymentservice.service;

import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import com.tamojit.paymentservice.dto.CreatePaymentRequest;
import com.tamojit.paymentservice.dto.PaymentOrderResponse;
import com.tamojit.paymentservice.model.Payment;
import com.tamojit.paymentservice.model.PaymentStatus;
import com.tamojit.paymentservice.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {
    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment.failed";

    @Value("${razorpay.key-id}")
    private String keyId;

    @Value("${razorpay.key-secret}")
    private String keySecret;

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Flow of Razorpay Payment Order:
     * -> Create order in razorpay
     * -> Save Payment Record in DB
     * -> Return order details to user (frontend)
     * -> Frontend shows razorpay checkout page from the details
     * -> User initiates payment
     * -> razorpay calls webhook for verification after payment
     */

    public PaymentOrderResponse createPaymentOrder(CreatePaymentRequest request) throws RazorpayException {
        log.info("Creating payment order for account: {}, amount: {}", request.getAccountNumber(), request.getAmount());

        RazorpayClient razorpayClient = new RazorpayClient(
            keyId,
            keySecret
        );

        // converting amt. to the smallest unit (INR -> paise)
        int convertedAmount = request.getAmount()
            .multiply(BigDecimal.valueOf(100))
            .intValue();

        // Creating a razorpay payment order
        JSONObject orderRequest = new JSONObject();
        orderRequest.put("amount", convertedAmount);
        orderRequest.put("currency", "INR");
        orderRequest.put("receipt", "rcpt_" + System.currentTimeMillis() + UUID.randomUUID().toString()
            .replace("-", "").substring(0, 10)); // unique Receipt ID

        Order razorpayOrder = razorpayClient.orders.create(orderRequest);

        log.info("Razorpay payment order created: {}", razorpayOrder.get("id").toString());

        // Saving payment record in DB
        Payment payment = new Payment();
        payment.setRazorpayOrderId(razorpayOrder.get("id").toString());
        payment.setAccountNumber(request.getAccountNumber());
        payment.setAmount(request.getAmount());
        payment.setCurrency("INR");
        payment.setStatus(PaymentStatus.CREATED);
        payment.setDescription(request.getDescription());

        Payment savedPayment = paymentRepository.save(payment);

        return new PaymentOrderResponse(
            savedPayment.getId(),
            razorpayOrder.get("id").toString(),
            request.getAmount(),
            "INR",
            "CREATED",
            keyId
        );
    }

    public void handleWebhook(Map<String, Object> payload) {
        log.info("Received Razorpay webhook: {}", payload.get("event"));

        String event = (String) payload.get("event");

        if ("payment.captured".equals(event)) {
            handlePaymentSuccess(payload);
        } else if ("payment.failed".equals(event)) {
            handlePaymentFailure(payload);
        }
    }

    private void handlePaymentSuccess(Map<String, Object> payload) {
        try {
            Map<String, Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");
            String paymentId = (String) paymentData.get("id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Payment not found for orderId: " + orderId));

            payment.setRazorpayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);
            paymentRepository.save(payment);

            // Publishing payment.completed event to Kafka
            Map<String, Object> event = new HashMap<>();
            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount", payment.getAmount());
            event.put("razorpayPaymentId", paymentId);

            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, payment.getId(), event);
            log.info("Payment completed: {}", payment.getId());
        } catch (Exception e) {
            log.error("Error while handling payment success event: ", e);
        }
    }

    private void handlePaymentFailure(Map<String, Object> payload) {
        try {
            Map<String, Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Payment not found for orderId: " + orderId));

            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment failed via Razorpay");
            paymentRepository.save(payment);

            // Publishing payment.failed event to Kafka
            Map<String, Object> event = new HashMap<>();
            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount", payment.getAmount());
            event.put("reason", payment.getFailureReason());

            kafkaTemplate.send(PAYMENT_FAILED_TOPIC, payment.getId(), event);
            log.info("Payment failed: {}", payment.getId());
        } catch (Exception e) {
            log.error("Error while handling payment failure event: ", e);
        }
    }

    private Map<String, Object> extractPaymentData(Map<String, Object> payload) {
        Map<String, Object> entity = (Map<String, Object>) payload.get("payload");

        Map<String, Object> paymentWrapper = (Map<String, Object>) entity.get("payment");

        return (Map<String, Object>) paymentWrapper.get("entity");
    }
}
