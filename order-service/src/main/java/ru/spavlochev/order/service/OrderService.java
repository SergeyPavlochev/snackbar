package ru.spavlochev.order.service;

import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.spavlochev.order.entity.Order;
import ru.spavlochev.order.openapi.dto.CreateOrderRequestDto;
import ru.spavlochev.order.openapi.dto.OrderDto;
import ru.spavlochev.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxService outboxService;

    @Transactional
    public OrderDto createOrder(UUID userId, CreateOrderRequestDto rqDto) {
        var amount = rqDto.getAmount();
        var currency = rqDto.getCurrency();
        log.info("Creating order for userId={}, amount={} {}", userId, amount, currency);

        Order order = Order.builder()
                .userId(userId)
                .amount(new BigDecimal(amount))
                .currency(currency)
                .status(Order.OrderStatus.PENDING)
                .build();

        order = orderRepository.save(order);
        outboxService.saveEvent(
                "OrderCreated", order.getId(), "Order",
                Map.of("orderId", order.getId().toString(),
                        "userId", userId.toString(),
                        "amount", order.getAmount().toPlainString(),
                        "currency", currency));

        log.info("Order and outbox event created: orderId={}, userId={}, status={}",
                order.getId(), userId, order.getStatus());

        return new OrderDto()
                .id(order.getId())
                .userId(order.getUserId())
                .amount(order.getAmount().toString())
                .currency(order.getCurrency())
                .status(OrderDto.StatusEnum.fromValue(order.getStatus().name()))
                .createdAt(order.getCreatedAt().atOffset(ZoneOffset.UTC));
    }

    @Transactional
    public void markAsPaid(UUID orderId) {
        log.info("Marking order as paid: orderId={}", orderId);

        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));

        if (order.getStatus() != Order.OrderStatus.PENDING) {
            log.warn("Order already processed: orderId={}, currentStatus={}", orderId, order.getStatus());
            return;
        }

        order.setStatus(Order.OrderStatus.PAID);
        orderRepository.save(order);

        log.info("Order marked as paid: orderId={}", orderId);
    }

    @Transactional
    public void markAsFailed(UUID orderId) {
        log.info("Marking order as failed: orderId={}", orderId);

        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));

        if (order.getStatus() != Order.OrderStatus.PENDING) {
            log.warn("Order already processed: orderId={}, currentStatus={}", orderId, order.getStatus());
            return;
        }

        order.setStatus(Order.OrderStatus.FAILED);
        orderRepository.save(order);

        log.info("Order marked as failed: orderId={}", orderId);
    }

    @Transactional(readOnly = true)
    public OrderDto getOrder(UUID userId, UUID orderId) {
        return orderRepository.findById(orderId)
                .filter(order -> userId.equals(order.getUserId()))
                .map(order -> new OrderDto()
                        .id(order.getId())
                        .userId(order.getUserId())
                        .amount(order.getAmount().toString())
                        .currency(order.getCurrency())
                        .status(OrderDto.StatusEnum.fromValue(order.getStatus().name()))
                        .createdAt(order.getCreatedAt().atOffset(ZoneOffset.UTC)))
                .orElseThrow(() -> new EntityNotFoundException("Order not found: " + orderId));
    }
}
