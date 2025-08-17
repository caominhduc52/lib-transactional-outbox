package com.duccao.learn.kafkalearning.config;

import com.duccao.learn.kafkalearning.config.property.ThreadPoolProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableAsync
@ConfigurationPropertiesScan
public class ThreadPoolConfig {
  @Bean("outboxEventProcessingPool")
  public ExecutorService outboxEventProcessingPool(ThreadPoolProperties threadPoolProperties) {
    ThreadPoolTaskExecutor executor = createThreadPoolExecutor(threadPoolProperties);
    return executor.getThreadPoolExecutor();
  }

  @Bean("kafkaProducerPool")
  public ExecutorService kafkaProducerPool(ThreadPoolProperties threadPoolProperties) {
    ThreadPoolTaskExecutor executor = createThreadPoolExecutor(threadPoolProperties);
    return executor.getThreadPoolExecutor();
  }

  private ThreadPoolTaskExecutor createThreadPoolExecutor(ThreadPoolProperties threadPoolProperties) {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(threadPoolProperties.corePoolSize());
    executor.setMaxPoolSize(threadPoolProperties.maxPoolSize());
    executor.setQueueCapacity(threadPoolProperties.queueCapacity());
    executor.setKeepAliveSeconds((int) threadPoolProperties.getKeepAliveTimeSeconds());
    executor.setThreadNamePrefix(threadPoolProperties.threadNamePrefix());
    executor.setAllowCoreThreadTimeOut(threadPoolProperties.allowCoreThreadTimeOut());
    executor.setRejectedExecutionHandler(getRejectedExecutionHandler(threadPoolProperties.rejectedExecutionHandler()));
    executor.initialize();
    return executor;
  }

  private RejectedExecutionHandler getRejectedExecutionHandler(String handlerName) {
    return switch (handlerName) {
      case "AbortPolicy" -> new ThreadPoolExecutor.AbortPolicy();
      case "DiscardPolicy" -> new ThreadPoolExecutor.DiscardPolicy();
      case "DiscardOldestPolicy" -> new ThreadPoolExecutor.DiscardOldestPolicy();
      default -> new ThreadPoolExecutor.CallerRunsPolicy();
    };
  }
}
