package ru.spavlochev.order.service;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import ru.spavlochev.snackbar.events.v1.EventEnvelope;
import ru.spavlochev.snackbar.events.v1.Money;
import ru.spavlochev.snackbar.events.v1.OrderCreatedEvent;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventPublisher {

    private static final String ORDERS_TOPIC = "orders";

    private final KafkaTemplate<String, EventEnvelope> kafkaTemplate;

    public void publishOrderCreated(String orderId, String userId, String amount, String currency) {
        OrderCreatedEvent payload = OrderCreatedEvent.newBuilder()
                .setOrderId(orderId)
                .setUserId(userId)
                .setAmount(Money.newBuilder()
                        .setAmount(amount)
                        .setCurrency(currency)
                        .build())
                .build();

        EventEnvelope envelope = EventEnvelope.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType("OrderCreated")
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(Instant.now().getEpochSecond())
                        .setNanos(Instant.now().getNano())
                        .build())
                .setCorrelationId(UUID.randomUUID().toString())
                .setSource("order-service")
                .setOrderCreated(payload)
                .build();

        CompletableFuture<SendResult<String, EventEnvelope>> future =
                kafkaTemplate.send(ORDERS_TOPIC, userId, envelope);
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
