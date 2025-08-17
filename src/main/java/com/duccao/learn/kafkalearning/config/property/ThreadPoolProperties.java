package com.duccao.learn.kafkalearning.config.property;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@ConfigurationProperties(prefix = "outbox.task.scheduler.publish.thread-pool")
@Validated
public record ThreadPoolProperties(
    @NotNull
    @Min(1)
    @DefaultValue("5")
    Integer corePoolSize,

    @NotNull
    @Min(1)
    @DefaultValue("10")
    Integer maxPoolSize,

    @NotNull
    @Min(0)
    @DefaultValue("100")
    Integer queueCapacity,

    @NotNull
    @DefaultValue("PT1M")
    Duration keepAliveTime,

    @NotNull
    @DefaultValue("async-task-")
    String threadNamePrefix,

    @DefaultValue("false")
    boolean allowCoreThreadTimeOut,

    @DefaultValue("CallerRunsPolicy")
    String rejectedExecutionHandler
) {

  public ThreadPoolProperties {
    if (maxPoolSize < corePoolSize) {
      throw new IllegalArgumentException("maxPoolSize must be greater than or equal to corePoolSize");
    }
  }

  public ThreadPoolProperties() {
    this(5, 10, 100, Duration.ofMinutes(1), "async-task-", false, "CallerRunsPolicy");
  }

  public long getKeepAliveTimeSeconds() {
    return keepAliveTime.getSeconds();
  }

  public boolean hasValidConfiguration() {
    return corePoolSize > 0 &&
        maxPoolSize > 0 &&
        maxPoolSize >= corePoolSize &&
        queueCapacity >= 0 &&
        keepAliveTime != null &&
        threadNamePrefix != null && !threadNamePrefix.isEmpty();
  }
}
