package ru.spavlochev.order.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import ru.spavlochev.order.entity.ProcessedEvent;
import ru.spavlochev.order.repository.ProcessedEventRepository;
import ru.spavlochev.snackbar.events.v1.EventEnvelope;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventHandler {

    private final OrderService orderService;
    private final ProcessedEventRepository processedEventRepository;
    private final TransactionExecutor transactionExecutor;

    @KafkaListener(topics = "payments", groupId = "${spring.kafka.consumer.group-id}")
    public void handlePaymentEvent(EventEnvelope envelope) {
        log.info("Received event: type={}, eventId={}", envelope.getEventType(), envelope.getEventId());

        try {
            if (envelope.hasOrderPaymentCompleted()) {
                processEventIdempotently(envelope, () -> handleOrderPaymentCompleted(envelope));
            } else if (envelope.hasOrderPaymentFailed()) {
                processEventIdempotently(envelope, () -> handleOrderPaymentFailed(envelope));
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

    /**
     * Универсальный метод идемпотентной обработки.
     * Проверяет, не обрабатывали ли мы уже это событие, и если нет — выполняет бизнес-логику
     * и сохраняет eventId в одной транзакции.
     */
    private void processEventIdempotently(EventEnvelope envelope, Runnable action) {
        UUID eventId = UUID.fromString(envelope.getEventId());

        // 1. Проверяем, не обработано ли уже событие
        boolean eventWasProcessed = transactionExecutor.execInTransaction(() ->
                processedEventRepository.existsByEventId(eventId));
        if (eventWasProcessed) {
            log.info("Event already processed, skipping: eventId={}, type={}",
                    eventId, envelope.getEventType());
            return;
        }

        // 2. Выполняем бизнес-логику и сохраняем eventId в одной транзакции
        // Если бизнес-логика упадет — транзакция откатится, eventId не сохранится
        // Если сохранение eventId упадет — откатится и бизнес-логика
        transactionExecutor.execInTransaction(() -> {
            log.info("Processing event: eventId={}, type={}", eventId, envelope.getEventType());
            action.run();
            processedEventRepository.save(ProcessedEvent.builder()
                    .eventId(eventId)
                    .eventType(envelope.getEventType())
                    .build());
            log.info("Event processed successfully: eventId={}", eventId);
        });
    }
}
