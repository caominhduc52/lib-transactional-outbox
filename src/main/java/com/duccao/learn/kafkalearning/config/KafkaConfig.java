package com.duccao.learn.kafkalearning.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.LoggingProducerListener;

import java.util.HashMap;
import java.util.Map;

@AutoConfiguration
public class KafkaConfig {

  @ConditionalOnMissingBean
  @Bean
  public ProducerFactory<byte[], byte[]> byteProducerFactory() {
    Map<String, Object> configurations = new HashMap<>();
    configurations.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    configurations.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    return new DefaultKafkaProducerFactory<>(configurations);
  }

  @ConditionalOnMissingBean
  @Bean
  public KafkaTemplate<byte[], byte[]> byteKafkaTemplate(ProducerFactory<byte[], byte[]> producerFactory) {
    KafkaTemplate<byte[], byte[]> kafkaTemplate = new KafkaTemplate<>(producerFactory);
    kafkaTemplate.setProducerListener(new LoggingProducerListener<>());
    return kafkaTemplate;
  }
}
