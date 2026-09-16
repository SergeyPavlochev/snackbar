package ru.spavlochev.notification.service;

import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.spavlochev.notification.entity.Notification;
import ru.spavlochev.notification.openapi.dto.NotificationDto;
import ru.spavlochev.notification.repository.NotificationRepository;

import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationService {

    private final NotificationRepository notificationRepository;

    @Transactional
    public void createOrderPaidNotification(UUID userId, UUID orderId, String amount) {
        log.info("Creating ORDER_PAID notification for userId={}, orderId={}", userId, orderId);

        Notification notification = new Notification();
        notification.setUserId(userId);
        notification.setType(Notification.NotificationType.ORDER_PAID);
        notification.setOrderId(orderId);
        notification.setAmount(amount);
        notification.setMessage("Ваш заказ успешно оплачен. Сумма: " + amount);

        notificationRepository.save(notification);

        log.info("Notification created: id={}, userId={}, type={}",
                notification.getId(), userId, notification.getType());
    }

    @Transactional
    public void createOrderFailedNotification(UUID userId, UUID orderId, String amount, String reason) {
        log.info("Creating ORDER_FAILED notification for userId={}, orderId={}, reason={}",
                userId, orderId, reason);

        Notification notification = new Notification();
        notification.setUserId(userId);
        notification.setType(Notification.NotificationType.ORDER_FAILED);
        notification.setOrderId(orderId);
        notification.setAmount(amount);
        notification.setMessage("Оплата заказа не удалась. Причина: " + reason);

        notificationRepository.save(notification);

        log.info("Notification created: id={}, userId={}, type={}",
                notification.getId(), userId, notification.getType());
    }

    @Transactional(readOnly = true)
    public List<NotificationDto> getNotifications(UUID userId, int limit) {
        log.info("Getting notifications for userId={}, limit={}", userId, limit);
        var notifications = notificationRepository.findByUserIdOrderByCreatedAtDesc(userId, PageRequest.of(0, limit))
                .stream()
                .map(notif -> new NotificationDto()
                        .id(notif.getId())
                        .userId(notif.getUserId())
                        .type(NotificationDto.TypeEnum.valueOf(notif.getType().name()))
                        .orderId(notif.getOrderId())
                        .amount(notif.getAmount())
                        .message(notif.getMessage()))
                .toList();

        if (notifications.isEmpty()) {
            throw new EntityNotFoundException("Уведомления пользователя не найдены");
        }
        return notifications;
    }
}
