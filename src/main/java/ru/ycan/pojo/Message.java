package ru.ycan.pojo;

import lombok.Builder;
import lombok.With;

/**
 * POJO класс сообщения между пользователями.
 *
 * @param userIdFrom  от кого сообщение
 * @param userIdTo    кому сообщение
 * @param messageText текст сообщения
 */
@With
@Builder
public record Message(String userIdFrom, String userIdTo, String messageText) {
}
