package com.kafka.consumer.configuration;

public interface Constants {
    String ENABLE_AUTO_COMMIT_PROPERTY = "enable.auto.commit";
    String KEY_DESERIALIZER_PROPERTY = "key.deserializer";
    String VALUE_DESERIALIZER_PROPERTY = "value.deserializer";
    String SESSION_TIMEOUT_MS_PROPERTY = "session.timeout.ms";
    String FETCH_MIN_BYTES_PROPERTY = "fetch.min.bytes";
    String RECEIVED_BUFFER_BYTES_PROPERTY = "receive.buffer.bytes";
    String MAX_PARTITION_FETCH_BYTES_PROPERTY = "max.partition.fetch.bytes";
    String AUTO_OFFSET_RESET_PROPERTY = "auto.offset.reset";
    String GROUP_ID_PROPERTY = "group.id";
    String BOOTSTRAP_SERVERS_PROPERTY = "bootstrap.servers";

    String AUTO_OFFSET_RESET_EARLIEST = "earliest";
}
