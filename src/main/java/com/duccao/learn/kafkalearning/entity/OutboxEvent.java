package com.duccao.learn.kafkalearning.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Instant;
import java.util.Objects;

@Entity(name = "outbox_event")
@Table(name = "outbox_event", indexes = {
    @Index(name = "idx_status_created", columnList = "status, createdTime"),
    @Index(name = "idx_idempotency_key", columnList = "idempotencyKey", unique = true)
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString(exclude = {"payload", "key"})
public class OutboxEvent {

  @Id
  @GeneratedValue(strategy = GenerationType.UUID)
  @Column(name = "id")
  private String id;

  @Column(name = "idempotency_key", updatable = false, nullable = false)
  @NotNull
  @NotBlank
  private String idempotencyKey;

  @Column(name = "published_time")
  private Instant publishedTime;

  @Column(name = "created_time", nullable = false, updatable = false)
  private Instant createdTime;

  @Column(name = "update_time")
  private Instant updateTime;

  @Column(name = "ack_time")
  private Instant ackTime;

  @Column(name = "event_type", nullable = false)
  @NotNull
  @NotBlank
  private String eventType;

  @Column(name = "partition_id")
  private String partitionId;

  @Column(name = "topic", nullable = false)
  @NotNull
  @NotBlank
  private String topic;

  @Column(name = "key", updatable = false)
  private byte[] key;

  @Column(name = "payload", updatable = false, nullable = false)
  @NotNull
  private byte[] payload;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false)
  private EventStatus status = EventStatus.PENDING;

  @Column(name = "retries", nullable = false)
  private Integer retries = 3;

  @Column(name = "last_retry_time")
  private Instant lastRetryTime;

  @Column(name = "error_message")
  private String errorMessage;

  @PrePersist
  protected void onCreate() {
    this.createdTime = Instant.now();
    this.updateTime = this.createdTime;
  }

  @PreUpdate
  protected void onUpdate() {
    this.updateTime = Instant.now();
  }

  public void markAsPublished(RecordMetadata metadata) {
    Objects.requireNonNull(metadata, "RecordMetadata cannot be null");
    this.status = EventStatus.PUBLISHED;
    this.publishedTime = Instant.ofEpochMilli(metadata.timestamp());
    this.ackTime = Instant.now();
    this.partitionId = String.valueOf(metadata.partition());
  }

  public void markAsRetrying() {
    if (!canRetry()) {
      markAsFailed("Maximum retry attempts exceeded");
      return;
    }

    this.retries = this.retries + 1;
    this.lastRetryTime = Instant.now();
    this.status = EventStatus.RETRYING;
    this.ackTime = Instant.now();
  }

  public void markAsFailed(String errorMessage) {
    this.status = EventStatus.FAILED;
    this.errorMessage = errorMessage;
  }

  private boolean canRetry() {
    return this.retries < 3;
  }

  public enum EventStatus {
    PENDING, PUBLISHED, FAILED, RETRYING
  }
}
