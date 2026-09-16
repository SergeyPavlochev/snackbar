package ru.spavlochev.auth.service;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import ru.spavlochev.snackbar.events.v1.EventEnvelope;
import ru.spavlochev.snackbar.events.v1.UserCreatedEvent;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class EventPublisher {

    private static final String USERS_TOPIC = "users";

    private final KafkaTemplate<String, EventEnvelope> kafkaTemplate;

    public EventPublisher(KafkaTemplate<String, EventEnvelope> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishUserCreated(String userId, String email) {
        UserCreatedEvent payload = UserCreatedEvent.newBuilder()
                .setUserId(userId)
                .setEmail(email)
                .build();

        EventEnvelope envelope = EventEnvelope.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType("UserCreated")
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(Instant.now().getEpochSecond())
                        .setNanos(Instant.now().getNano())
                        .build())
                .setCorrelationId(UUID.randomUUID().toString())
                .setSource("auth-service")
                .setUserCreated(payload)
                .build();

        CompletableFuture<SendResult<String, EventEnvelope>> future = kafkaTemplate.send(USERS_TOPIC, userId, envelope);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish event: eventId={}", envelope.getEventId(), ex);
            } else {
                log.info("Event published: eventId={}, topic={}, partition={}, offset={}",
                        envelope.getEventId(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
