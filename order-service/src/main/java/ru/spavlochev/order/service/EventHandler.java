package ru.spavlochev.order.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import ru.spavlochev.snackbar.events.v1.EventEnvelope;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventHandler {

    private final OrderService orderService;

    @KafkaListener(topics = "payments", groupId = "${spring.kafka.consumer.group-id}")
    public void handlePaymentEvent(EventEnvelope envelope) {
        log.info("Received event: type={}, eventId={}", envelope.getEventType(), envelope.getEventId());

        try {
            if (envelope.hasOrderPaymentCompleted()) {
                handleOrderPaymentCompleted(envelope);
            } else if (envelope.hasOrderPaymentFailed()) {
                handleOrderPaymentFailed(envelope);
            } else {
                log.warn("Unexpected payload type in payments topic: {}", envelope.getEventType());
            }
        } catch (Exception e) {
            log.error("Failed to process payment event: eventId={}, type={}",
                    envelope.getEventId(), envelope.getEventType(), e);
            throw e;
        }
    }

    private void handleOrderPaymentCompleted(EventEnvelope envelope) {
        var event = envelope.getOrderPaymentCompleted();
        log.info("Processing OrderPaymentCompleted: orderId={}", event.getOrderId());

        orderService.markAsPaid(UUID.fromString(event.getOrderId()));
    }

    private void handleOrderPaymentFailed(EventEnvelope envelope) {
        var event = envelope.getOrderPaymentFailed();
        log.info("Processing OrderPaymentFailed: orderId={}, reason={}",
                event.getOrderId(), event.getReason());

        orderService.markAsFailed(UUID.fromString(event.getOrderId()));
    }
}
