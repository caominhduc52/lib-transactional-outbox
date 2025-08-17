package com.duccao.learn.kafkalearning.service;

import com.duccao.learn.kafkalearning.domain.OutboxEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
@Getter
public class SerializerService {

  @Value("${schema.registry.url}")
  private String schemaRegistryUrl;

  private String subjectValueNameStrategy;
  private String subjectKeyNameStrategy;

  private KafkaAvroSerializer kafkaAvroValueSerializer;
  private KafkaAvroSerializer kafkaAvroKeySerializer;
  private KafkaAvroDeserializer avroDeserializer;

  @PostConstruct
  public void init() {
    log.debug("message=\"Init MessageSerializerService\" schemaRegistryUrl={} subjectValueNameStrategy={} subjectKeyNameStrategy={}",
        schemaRegistryUrl, subjectValueNameStrategy, subjectKeyNameStrategy);
    Map<String, String> configurations = new HashMap<>();
    configurations.put("schema.registry.url", schemaRegistryUrl);
    configurations.put("auto.register.schemas", "false");

    // TODO: go with default for now, this should be configurable in future
//    configurations.put("value.subject.name.strategy", subjectValueNameStrategy);
//    configurations.put("key.subject.name.strategy", subjectKeyNameStrategy);

    kafkaAvroKeySerializer = new KafkaAvroSerializer();
    kafkaAvroKeySerializer.configure(configurations, true);

    kafkaAvroValueSerializer = new KafkaAvroSerializer();
    kafkaAvroValueSerializer.configure(configurations, false);

    avroDeserializer = new KafkaAvroDeserializer();
    avroDeserializer.configure(configurations, false);
  }

  private byte[] serializeValue(String topic, Object object) {
    log.debug("message=\"Serialize value object: {}\"", object);
    return kafkaAvroValueSerializer.serialize(topic, object);
  }

  private byte[] serializeKey(String topic, Object key) {
    log.debug("message=\"Serialize key object: {}\"", key);
    return kafkaAvroKeySerializer.serialize(topic, key);
  }

  public Object deserialize(String topic, byte[] data) {
    log.debug("message=\"Deserialize value from topic: {}\"", topic);
    return avroDeserializer.deserialize(topic, data);
  }

  public byte[] serializeKey(OutboxEvent event) {
    Object key = event.key();
    if (key == null) {
      throw new IllegalArgumentException("Event key cannot be null");
    }

    if (key instanceof String stringKey) {
      return stringKey.getBytes(StandardCharsets.UTF_8);
    } else if (key instanceof byte[] byteKey) {
      return byteKey;
    } else {
      return serializeKey(event.topic(), key);
    }
  }

  public byte[] serializePayload(OutboxEvent event) {
    Object payload = event.value();
    if (payload == null) {
      throw new IllegalArgumentException("Event key cannot be null");
    }

    if (payload instanceof String stringPayload) {
      return stringPayload.getBytes(StandardCharsets.UTF_8);
    } else if (payload instanceof byte[] bytePayload) {
      return bytePayload;
    } else {
      return serializeValue(event.topic(), payload);
    }
  }
}
