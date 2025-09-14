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

  public ExecutorServiceWrapper newExecutorWrapper() {
    return new ExecutorServiceWrapper(outboxEventProcessingPool, kafkaProducerPool);
  }

  public record ExecutorServiceWrapper(ExecutorService eventProcessingPool,
                                       ExecutorService kafkaProducerPool) implements AutoCloseable {
    @Override
    public void close() {
      shutdownExecutorService(eventProcessingPool);
      shutdownExecutorService(kafkaProducerPool);
    }

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
      } catch (Exception e) {
        log.error("message=\"Error closing executor services\"", e);
      }
    }
  }
}
