package ru.spavlochev.notification.service;

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

    private final NotificationService notificationService;

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
        log.info("Processing OrderPaymentCompleted: orderId={}, userId={}",
                event.getOrderId(), event.getUserId());

        notificationService.createOrderPaidNotification(
                UUID.fromString(event.getUserId()),
                UUID.fromString(event.getOrderId()),
                event.getAmount().getAmount() + " " + event.getAmount().getCurrency()
        );
    }

    private void handleOrderPaymentFailed(EventEnvelope envelope) {
        var event = envelope.getOrderPaymentFailed();
        log.info("Processing OrderPaymentFailed: orderId={}, userId={}, reason={}",
                event.getOrderId(), event.getUserId(), event.getReason());

        notificationService.createOrderFailedNotification(
                UUID.fromString(event.getUserId()),
                UUID.fromString(event.getOrderId()),
                event.getAmount().getAmount() + " " + event.getAmount().getCurrency(),
                event.getReason()
        );
    }
}