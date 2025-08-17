package com.duccao.learn.kafkalearning.service;

import com.duccao.learn.kafkalearning.entity.OutboxEvent;
import com.duccao.learn.kafkalearning.helper.ExecutorHelper;
import com.duccao.learn.kafkalearning.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

@Service
@Slf4j
@RequiredArgsConstructor
public class OutboxService {

  @Value("${outbox.task.scheduler.publish.batch-size:500}")
  private int publishBatchSize;

  @Value("${outbox.task.scheduler.publish.thread-pool-size:10}")
  private int publishThreadPoolSize;

  @Value("${outbox.task.scheduler.acknowledge-timeout:PT010S}")
  private Duration acknowledgeTimeout;

  private final OutboxEventRepository outboxEventRepository;
  private final KafkaTemplate<byte[], byte[]> kafkaTemplate;
  private final SerializerService serializerService;
  private final ExecutorHelper executorHelper;

  @Scheduled(
      fixedRateString = "${outbox.task.scheduler.publish.fixed-rate:10000}",
      initialDelayString = "${outbox.task.scheduler.initial-delay:0}")
  @SchedulerLock(
      name = "${outbox.task.shedlock.publish.name:TaskScheduler_publishEventTask}",
      lockAtLeastFor = "${outbox.task.shedlock.publish.lock-at-most-for:PT14M",
      lockAtMostFor = "${outbox.task.shedlock.publish.lock-at-most-for:PT14M")
  public void publishToKafka() {
    ExecutorService eventProcessingPool = executorHelper.getOutboxEventProcessingPool();
    ExecutorService kafkaProducerPool = executorHelper.getKafkaProducerPool();

    try {
      Instant fetchStartTime = Instant.now();
      List<OutboxEvent> unpublishedEvents = outboxEventRepository.findUnpublishedEvents(publishBatchSize);
      log.debug("message=\"Fetched unpublished events\" count={} executionTimeMs={}",
          unpublishedEvents.size(),
          Duration.between(fetchStartTime, Instant.now()).toMillis());

      if (unpublishedEvents.isEmpty()) {
        log.info("message=\"No unpublished events found. Skip processing.\"");
        return;
      }

      Instant kafkaPublishStartTime = Instant.now();
      List<CompletableFuture<Void>> eventPublishTasks = unpublishedEvents.stream()
          .map(event -> CompletableFuture.runAsync(() -> {
            log.debug("message=\"Start publishing event\" eventId={} topic={}",
                event.getId(), event.getTopic());

            byte[] payload = event.getPayload();
            byte[] key = event.getKey();
            String topic = event.getTopic();

            kafkaTemplate.send(topic, key, payload)
                .thenAcceptAsync(sendResult -> {
                  log.debug("message=\"Event published successfully\" eventId={} topic={} partition={} offset={}",
                      event.getId(), topic, sendResult.getRecordMetadata().partition(),
                      sendResult.getRecordMetadata().offset());
                  event.markAsPublished(sendResult.getRecordMetadata());
                }, kafkaProducerPool)
                .exceptionallyAsync(exception -> {
                  log.error("message=\"Failed to publish event\" eventId={} topic={} error={}",
                      event.getId(), topic, exception.getMessage(), exception);
                  event.markAsRetrying();
                  return null;
                }, kafkaProducerPool)
                .join();
          }, eventProcessingPool))
          .toList();

      CompletableFuture.allOf(eventPublishTasks.toArray(new CompletableFuture[0])).join();
      log.debug("message=\"Successfully published to Kafka\" batchSize={} publishedCount={} executionTimeMs={}",
          publishBatchSize,
          unpublishedEvents.size(),
          Duration.between(kafkaPublishStartTime, Instant.now()).toMillis());

      Instant databaseUpdateStartTime = Instant.now();
      outboxEventRepository.saveAllAndFlush(unpublishedEvents);
      log.debug("message=\"Updated outbox events after publishing\" updatedCount={} executionTimeMs={}",
          unpublishedEvents.size(),
          Duration.between(databaseUpdateStartTime, Instant.now()).toMillis());
    } finally {
      executorHelper.shutdownExecutorService(eventProcessingPool, "eventProcessingPool");
      executorHelper.shutdownExecutorService(kafkaProducerPool, "kafkaProducerPool");
    }
  }

  public void saveToOutboxTable(com.duccao.learn.kafkalearning.domain.OutboxEvent<?, ?> outboxEvent) {
    try {
      OutboxEvent event = OutboxEvent.builder()
          .id(UUID.randomUUID().toString())
          .idempotencyKey(outboxEvent.idempotencyKey())
          .eventType(outboxEvent.eventType())
          .topic(outboxEvent.topic())
          .retries(0)
          .status(OutboxEvent.EventStatus.PENDING)
          .key(serializerService.serializeKey(outboxEvent))
          .payload(serializerService.serializePayload(outboxEvent))
          .build();

      log.debug("message=\"Saving outboxEvent to outbox table\" eventType={} topic={} idempotencyKey={}",
          outboxEvent.eventType(),
          outboxEvent.topic(),
          outboxEvent.idempotencyKey());
      outboxEventRepository.save(event);
    } catch (Exception e) {
      log.error("message=\"Failed to save outboxEvent to outbox table\" eventType={} topic={} error={}",
          outboxEvent.eventType(),
          outboxEvent.topic(),
          e.getMessage(),
          e);
      throw new RuntimeException("Could not persist data to outbox outboxEvent table: "
          + outboxEvent.idempotencyKey(), e.getCause());
    }
  }
}
