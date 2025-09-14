package com.duccao.learn.kafkalearning.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Entity(name = "lock")
@Table(name = "lock")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Lock {

  @Id
  @Column(name = "id")
  @GeneratedValue(strategy = GenerationType.UUID)
  private String id;

  @Column(name = "name", nullable = false, unique = true)
  private String name;

  @Column(name = "lock_until")
  private Instant lockUntil;

  @Column(name = "locked_at")
  private Instant lockedAt;

  @Column(name = "locked_by")
  private String lockedBy;
}
