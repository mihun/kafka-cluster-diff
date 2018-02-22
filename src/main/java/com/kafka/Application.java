package com.kafka;

import com.kafka.service.KafkaApplicationService;
import com.kafka.spring.StaticContextHolder;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;


@SpringBootApplication
@EnableScheduling
public class Application  implements ApplicationRunner {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(ApplicationArguments args)  {
        StaticContextHolder.getBean(KafkaApplicationService.class).run();
        System.exit(0);
    }

}