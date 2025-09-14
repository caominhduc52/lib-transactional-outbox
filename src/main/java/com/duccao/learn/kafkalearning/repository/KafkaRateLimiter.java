package com.duccao.learn.kafkalearning.repository;

import com.duccao.learn.kafkalearning.domain.OutboxEvent;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.internal.AtomicRateLimiter;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

/**
 * This is an idea for now, not using in any use case
 */
@Slf4j
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class KafkaRateLimiter {
  private final String topicName;
  private final RateLimiter rateLimiter;

  public void waitForPermission(List<OutboxEvent<?, ?>> events) {
    var metrics = (AtomicRateLimiter.AtomicRateLimiterMetrics) rateLimiter.getMetrics();
    log.debug("message=\"Waiting for permission to {}, nanosToWait={}, cycle={}, availablePermission={}\"", rateLimiter.getName(), metrics.getNanosToWait(), metrics.getCycle(), metrics.getAvailablePermissions());
    RateLimiter.waitForPermission(rateLimiter, toPermits(events));
  }

  protected abstract int toPermits(List<OutboxEvent<?, ?>> events);

  public static KafkaRateLimiter buildKafkaRateLimiter(String topicName, RateLimiter rateLimiter) {
    return new KafkaRateLimiter(topicName, rateLimiter) {
      @Override
      protected int toPermits(List<OutboxEvent<?, ?>> events) {
        return Optional.ofNullable(events)
            .map(List::size)
            .orElse(0);
      }
    };
  }
}
