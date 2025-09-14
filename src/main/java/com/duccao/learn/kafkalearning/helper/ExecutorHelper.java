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

  /**
   * Creates a new ExecutorServiceWrapper that can be used in a try-with-resources block.
   * This allows for proper resource cleanup without closing the Spring-managed ExecutorHelper bean.
   *
   * @return A new ExecutorServiceWrapper instance
   */
  public ExecutorServiceWrapper newExecutorWrapper() {
    return new ExecutorServiceWrapper(outboxEventProcessingPool, kafkaProducerPool);
  }

  /**
   * A wrapper class for executor services that implements AutoCloseable.
   * This allows for proper resource cleanup in a try-with-resources block.
   */
  public record ExecutorServiceWrapper(ExecutorService eventProcessingPool,
                                       ExecutorService kafkaProducerPool) implements AutoCloseable {
    @Override
    public void close() {
      try {
        shutdownExecutorService(eventProcessingPool);
        shutdownExecutorService(kafkaProducerPool);
      } catch (Exception e) {
        log.error("message=\"Error closing executor services\"", e);
      }
    }

    /**
     * Shuts down an executor service gracefully, waiting for tasks to complete.
     * If tasks don't complete within the timeout, they are forcibly cancelled.
     *
     * @param executor The executor service to shut down
     */
    private void shutdownExecutorService(ExecutorService executor) {
      try {
        executor.shutdown();
        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
          log.warn("message=\"Executor did not terminate gracefully\"");
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        executor.shutdownNow();
      }
    }
  }
}
