package com.kafka.consumer.reader;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.Properties;

@Component
public class PropertiesReader {

    public PropertiesConfiguration readPropertiesConfiguration(String path){
        try (InputStream props = new ClassPathResource(path).getInputStream()) {
            PropertiesConfiguration properties = new PropertiesConfiguration();
            properties.load(props);
            return properties;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public Properties readProperties(String path) {
        try (InputStream props = new ClassPathResource(path).getInputStream()) {
            Properties properties = new Properties();
            properties.load(props);
            return properties;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
