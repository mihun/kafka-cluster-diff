package com.kafka.consumer.configuration;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.core.io.ClassPathResource;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class ConsumerConfiguration {

    public static final Properties BACKUP_CONSUMER_PROPERTIES;
    public static final Properties PRODUCTION_CONSUMER_PROPERTIES;
    public static final int NUMBER_OF_THREADS;
    public static List<Object> excludeTopicList;


    static {
        PropertiesConfiguration properties = readCustomProperties();
        BACKUP_CONSUMER_PROPERTIES = formProperties(properties.getString("target_host"));
        PRODUCTION_CONSUMER_PROPERTIES = formProperties(properties.getString("source_host"));
        NUMBER_OF_THREADS = properties.getInt("threads");
        excludeTopicList = properties.getList("exclude_topics");
    }

    private static Properties formProperties(String host) {
        Properties properties = new Properties();
        properties.put(Constants.ENABLE_AUTO_COMMIT_PROPERTY, true);
        properties.put(Constants.KEY_DESERIALIZER_PROPERTY, ByteArrayDeserializer.class.getName());
        properties.put(Constants.VALUE_DESERIALIZER_PROPERTY, ByteArrayDeserializer.class.getName());
        properties.put(Constants.SESSION_TIMEOUT_MS_PROPERTY, 10000);
        properties.put(Constants.FETCH_MIN_BYTES_PROPERTY, 50000);
        properties.put(Constants.RECEIVED_BUFFER_BYTES_PROPERTY, 262144);
        properties.put(Constants.MAX_PARTITION_FETCH_BYTES_PROPERTY, 2097152);
        properties.put(Constants.AUTO_OFFSET_RESET_PROPERTY, Constants.AUTO_OFFSET_RESET_EARLIEST);
        properties.put(Constants.GROUP_ID_PROPERTY, UUID.randomUUID().toString());
        properties.put(Constants.BOOTSTRAP_SERVERS_PROPERTY, host);
        return properties;
    }

    private static PropertiesConfiguration readCustomProperties(){
        try (InputStream props = new ClassPathResource("custom.properties").getInputStream()) {
            PropertiesConfiguration properties = new PropertiesConfiguration();
            properties.load(props);
            return properties;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

}
