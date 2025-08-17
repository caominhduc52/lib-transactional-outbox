package com.duccao.learn.kafkalearning.domain;

public record OutboxEvent<K, V>(
    String topic,
    String eventType,
    String idempotencyKey,
    K key,
    V value
) {}
