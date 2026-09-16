package ru.spavlochev.billing.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import ru.spavlochev.snackbar.events.v1.EventEnvelope;

import java.math.BigDecimal;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class EventHandler {

    private final AccountService accountService;

    @KafkaListener(topics = "users", groupId = "${spring.kafka.consumer.group-id}")
    public void handleUserCreated(EventEnvelope envelope) {
        log.info("Received event: type={}, eventId={}", envelope.getEventType(), envelope.getEventId());

        if (!envelope.hasUserCreated()) {
            log.warn("Unexpected payload type in users topic: {}", envelope.getEventType());
            return;
        }

        var event = envelope.getUserCreated();
        try {
            accountService.createAccount(UUID.fromString(event.getUserId()));
        } catch (Exception e) {
            log.error("Failed to process UserCreated event: eventId={}", envelope.getEventId(), e);
            throw e;
        }
    }

    @KafkaListener(topics = "orders", groupId = "${spring.kafka.consumer.group-id}")
    public void handleOrderCreated(EventEnvelope envelope) {
        log.info("Received event: type={}, eventId={}", envelope.getEventType(), envelope.getEventId());

        if (!envelope.hasOrderCreated()) {
            log.warn("Unexpected payload type in orders topic: {}", envelope.getEventType());
            return;
        }

        var event = envelope.getOrderCreated();
        try {
            accountService.processOrderPayment(
                    event.getOrderId(),
                    UUID.fromString(event.getUserId()),
                    new BigDecimal(event.getAmount().getAmount()),
                    event.getAmount().getCurrency()
            );
        } catch (Exception e) {
            log.error("Failed to process OrderCreated event: eventId={}", envelope.getEventId(), e);
            throw e;
        }
    }
}
