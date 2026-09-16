package ru.spavlochev.billing.service;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import ru.spavlochev.snackbar.events.v1.EventEnvelope;
import ru.spavlochev.snackbar.events.v1.Money;
import ru.spavlochev.snackbar.events.v1.OrderPaymentCompletedEvent;
import ru.spavlochev.snackbar.events.v1.OrderPaymentFailedEvent;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class EventPublisher {

    private static final String PAYMENTS_TOPIC = "payments";

    private final KafkaTemplate<String, EventEnvelope> kafkaTemplate;

    public EventPublisher(KafkaTemplate<String, EventEnvelope> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishOrderPaymentCompleted(String orderId, String userId, String amount, String currency) {
        OrderPaymentCompletedEvent payload = OrderPaymentCompletedEvent.newBuilder()
                .setOrderId(orderId)
                .setUserId(userId)
                .setAmount(Money.newBuilder()
                        .setAmount(amount)
                        .setCurrency(currency)
                        .build())
                .build();

        EventEnvelope envelope = createEnvelope("OrderPaymentCompleted", userId, payload);
        sendToKafka(userId, envelope);
    }

    public void publishOrderPaymentFailed(String orderId, String userId, String amount, String currency, String reason) {
        OrderPaymentFailedEvent payload = OrderPaymentFailedEvent.newBuilder()
                .setOrderId(orderId)
                .setUserId(userId)
                .setAmount(Money.newBuilder()
                        .setAmount(amount)
                        .setCurrency(currency)
                        .build())
                .setReason(reason)
                .build();

        EventEnvelope envelope = createEnvelope("OrderPaymentFailed", userId, payload);
        sendToKafka(userId, envelope);
    }

    private EventEnvelope createEnvelope(String eventType, String correlationId, Object payloadCase) {
        EventEnvelope.Builder builder = EventEnvelope.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(eventType)
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(Instant.now().getEpochSecond())
                        .setNanos(Instant.now().getNano())
                        .build())
                .setCorrelationId(correlationId)
                .setSource("billing-service");

        // Устанавливаем payload в зависимости от типа
        if (payloadCase instanceof OrderPaymentCompletedEvent completed) {
            builder.setOrderPaymentCompleted(completed);
        } else if (payloadCase instanceof OrderPaymentFailedEvent failed) {
            builder.setOrderPaymentFailed(failed);
        }

        return builder.build();
    }

    private void sendToKafka(String key, EventEnvelope envelope) {
        CompletableFuture<SendResult<String, EventEnvelope>> future =
                kafkaTemplate.send(EventPublisher.PAYMENTS_TOPIC, key, envelope);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish event: eventId={}, type={}",
                        envelope.getEventId(), envelope.getEventType(), ex);
            } else {
                log.info("Event published: eventId={}, type={}, topic={}, partition={}, offset={}",
                        envelope.getEventId(),
                        envelope.getEventType(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
