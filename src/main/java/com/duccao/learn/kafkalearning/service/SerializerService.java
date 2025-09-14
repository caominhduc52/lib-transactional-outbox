package com.duccao.learn.kafkalearning.service;

import com.duccao.learn.kafkalearning.domain.OutboxEvent;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Service
@Slf4j
@Getter
public class SerializerService {

  @Value("${outbox.kafka.schema.registry.url}")
  private String schemaRegistryUrl;

  @Value("${outbox.kafka.subject.value-name-strategy}")
  private String subjectValueNameStrategy;

  @Value("${outbox.kafka.subject.key-name-strategy}")
  private String subjectKeyNameStrategy;

  private KafkaAvroSerializer kafkaAvroValueSerializer;
  private KafkaAvroSerializer kafkaAvroKeySerializer;

  @PostConstruct
  public void init() {
    log.debug("message=\"Init MessageSerializerService\" schemaRegistryUrl={} subjectValueNameStrategy={} subjectKeyNameStrategy={}",
        schemaRegistryUrl, subjectValueNameStrategy, subjectKeyNameStrategy);
    Map<String, String> configurations = new HashMap<>();
    configurations.put("schema.registry.url", schemaRegistryUrl);
    configurations.put("schema.registry.ssl.endpoint.identification.algorithm", "");
    configurations.put("auto.register.schemas", "false");
    configurations.put("value.subject.name.strategy", subjectValueNameStrategy);
    configurations.put("key.subject.name.strategy", subjectKeyNameStrategy);

    kafkaAvroKeySerializer = new KafkaAvroSerializer();
    kafkaAvroKeySerializer.configure(configurations, true);

    kafkaAvroValueSerializer = new KafkaAvroSerializer();
    kafkaAvroValueSerializer.configure(configurations, false);
  }

  private byte[] serializeValue(String topic, Object object) {
    log.debug("message=\"Serialize value object: {}\"", object);
    return kafkaAvroValueSerializer.serialize(topic, object);
  }

  private byte[] serializeKey(String topic, Object key) {
    log.debug("message=\"Serialize key object: {}\"", key);
    return kafkaAvroKeySerializer.serialize(topic, key);
  }

  public byte[] serializeKey(OutboxEvent<?, ?> event) {
    Object key = Objects.requireNonNull(event.key(), "Key cannot be null");
    if (key instanceof String stringKey) {
      return stringKey.getBytes(StandardCharsets.UTF_8);
    }

    if (key instanceof byte[] byteKey) {
      return byteKey;
    }

    return serializeKey(event.topic(), key);
  }

  public byte[] serializePayload(OutboxEvent<?, ?> event) {
    Object payload = Objects.requireNonNull(event.value(), "Payload cannot be null");
    if (payload instanceof String stringPayload) {
      return stringPayload.getBytes(StandardCharsets.UTF_8);
    }

    if (payload instanceof byte[] bytePayload) {
      return bytePayload;
    }

    return serializeValue(event.topic(), payload);
  }
}
