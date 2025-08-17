package com.duccao.learn.kafkalearning.repository;

import com.duccao.learn.kafkalearning.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {

  @Query(value = """
      SELECT * 
      FROM outbox_event o 
      WHERE o.published_time IS NULL 
      ORDER BY o.created_time ASC 
      LIMIT :batchSize
      """, nativeQuery = true)
  List<OutboxEvent> findUnpublishedEvents(@Param("batchSize") int batchSize);

  @Modifying
  @Query(value = """
      DELETE 
      FROM outbox_event 
      WHERE id IN (
          SELECT id 
          FROM outbox_event ev 
          WHERE ev.publishedTime IS NOT NULL 
            AND ev.status = 'PUBLISHED' 
          ORDER BY ev.publishedTime ASC 
          LIMIT :batchSize
      )""", nativeQuery = true
  )
  int cleanupPublishedEvents(@Param("batchSize") int batchSize);

  @Modifying
  @Query(value = """
      DELETE FROM outbox_event 
      WHERE id IN (
          SELECT subq.id FROM (
              SELECT id 
              FROM outbox_event 
              WHERE published_time IS NOT NULL 
                AND status = 'PUBLISHED'
                AND published_time < :priorToDate
              ORDER BY published_time ASC 
              LIMIT :batchSize
          )
      )
      """, nativeQuery = true)
  int cleanupPublishedEventsPriorToDate(@Param("batchSize") int batchSize, @Param("priorToDate") Instant priorToDate);
}
