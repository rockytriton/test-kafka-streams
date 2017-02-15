package com.qat.samples.kafka.boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.support.SpringBootServletInitializer;

/**
 * Created by rpulley on 2/9/17.
 */

@SpringBootApplication
public class BootApp extends SpringBootServletInitializer {

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(BootApp.class);
    }

    public static void main(String[] args) {
        SpringApplication.run(BootApp.class, args);
    }

}
