package com.kafka;

import com.kafka.consumer.configuration.ConsumerConfiguration;
import com.kafka.consumer.configuration.CustomConfiguration;
import com.kafka.service.KafkaApplicationService;
import com.kafka.spring.StaticContextHolder;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.FileReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import static java.lang.String.format;
import static java.lang.System.exit;


@SpringBootApplication
@EnableScheduling
public class Application  implements ApplicationRunner {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(ApplicationArguments arguments) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<String> backupConsumerConfig = parser.accepts("backup-consumer.config",
                "Consumer configuration for backup cluster.")
                .withRequiredArg()
                .ofType(String.class);
        OptionSpec<String> sourceConsumerConfig = parser.accepts("source-consumer.config",
                "Consumer configuration for source cluster.")
                .withRequiredArg()
                .ofType(String.class);

        OptionSpec<Integer> numberOfThreads = parser.accepts("threads",
                "Number of production threads.")
                .withRequiredArg()
                .ofType(Integer.class)
                .defaultsTo(10);

        OptionSpec<Integer> pollTimeoutMs = parser.accepts("poll-timeout-ms",
                "Poll timeout for consumer.")
                .withRequiredArg()
                .ofType(Integer.class)
                .defaultsTo(300);

        OptionSpec<String> customProperties = parser.accepts("custom.properties",
                "Ignored list of topics.")
                .withRequiredArg()
                .ofType(String.class);

        OptionSpec<Integer> bufferSize = parser.accepts("buffer-size",
                "Number of production threads.")
                .withRequiredArg()
                .ofType(Integer.class)
                .defaultsTo(2000);

        OptionSpec help = parser.accepts("help", "Print this message.");

        String[] args = arguments.getSourceArgs();
        OptionSet options = parser.parse(args);

        if (args.length == 0 || options.has(help)) {
            parser.printHelpOn(System.out);
            exit(0);
        }

        for (OptionSpec<?> requiredOption : Arrays.asList(backupConsumerConfig, sourceConsumerConfig)) {
            if (!options.has(requiredOption)) {
                System.out.println(format("Missing required argument: %s", requiredOption));
                parser.printHelpOn(System.out);
                exit(0);
            }
        }

        Properties backupConsumerProperties = new Properties();
        try (Reader reader = new FileReader(options.valueOf(backupConsumerConfig))) {
            backupConsumerProperties.load(reader);
            backupConsumerProperties.put("group.id", UUID.randomUUID().toString());
        }

        Properties sourceConsumerProperties = new Properties();
        try (Reader reader = new FileReader(options.valueOf(sourceConsumerConfig))) {
            sourceConsumerProperties.load(reader);
            sourceConsumerProperties.put("group.id", UUID.randomUUID().toString());
        }

        PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration();
        try (Reader reader = new FileReader(options.valueOf(sourceConsumerConfig)))  {
            propertiesConfiguration.load(reader);
        }

        ConsumerConfiguration consumerConfiguration = StaticContextHolder.getBean(ConsumerConfiguration.class);
        consumerConfiguration.setBackupConsumerProperties(backupConsumerProperties);
        consumerConfiguration.setSourceConsumerProperties(sourceConsumerProperties);

        CustomConfiguration customConfiguration = StaticContextHolder.getBean(CustomConfiguration.class);
        customConfiguration.setBufferSize(options.valueOf(bufferSize));
        customConfiguration.setNumberOfThreads(options.valueOf(numberOfThreads));
        customConfiguration.setPollTimeout(options.valueOf(pollTimeoutMs));
        customConfiguration.setExcludeTopicList(propertiesConfiguration.getList("exclude-topics"));

        StaticContextHolder.getBean(KafkaApplicationService.class).run();
        System.exit(0);
    }

}