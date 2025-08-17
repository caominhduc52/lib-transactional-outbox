package com.duccao.learn.kafkalearning.config;

import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.sql.DataSource;

@Configuration
@EnableScheduling
@EntityScan("com.duccao.learn.kafkalearning.entity")
@EnableJpaRepositories("com.duccao.learn.kafkalearning.repository")
@ComponentScan("com.duccao.learn.kafkalearning.service")
@ConfigurationPropertiesScan("com.duccao.learn.kafkalearning.config")
@EnableSchedulerLock(defaultLockAtMostFor = "${outbox.task.shedlock.publish.defaultLockAtMostFor:PT30S}")
public class SchedulerConfig {

  @Bean
  public LockProvider lockProvider(DataSource dataSource) {
    JdbcTemplateLockProvider.Configuration lockConfigurations = JdbcTemplateLockProvider.Configuration.builder()
        .withJdbcTemplate(new JdbcTemplate(dataSource))
        .withTableName("lock")
        .usingDbTime()
        .build();
    return new JdbcTemplateLockProvider(lockConfigurations);
  }
}
