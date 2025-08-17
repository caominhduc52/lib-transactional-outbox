package com.duccao.learn.kafkalearning.helper;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
@Getter
public class ExecutorHelper {
  private final ExecutorService outboxEventProcessingPool;
  private final ExecutorService kafkaProducerPool;

  public void shutdownExecutorService(ExecutorService executor, String name) {
    try {
      executor.shutdown();
      if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
        log.warn("message=\"Executor {} did not terminate gracefully\"", name);
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      executor.shutdownNow();
    }
  }
}
