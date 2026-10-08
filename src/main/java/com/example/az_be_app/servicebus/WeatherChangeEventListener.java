package com.example.az_be_app.servicebus;

import org.springframework.stereotype.Component;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.spring.messaging.servicebus.implementation.core.annotation.ServiceBusListener;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class WeatherChangeEventListener {

    @ServiceBusListener(id = "axb2asb-c1", destination = "generic_events", group = "subscription_one")
    public void handleWeatherChangeEvent(ServiceBusReceivedMessage message) {

        log.info("WeatherChangeEventListener [axb2asb-c1] >> {}", message);
        log.info("WeatherChangeEventListener [axb2asb-c1] >> {}",
                message.getApplicationProperties().getOrDefault("traceparent", "Not Found"));
        log.info("WeatherChangeEventListener [axb2asb-c1] >> {}",
                message.getApplicationProperties().getOrDefault("Diagnostic-Id", "Not Found"));
        throw new RuntimeException("Just Cause We Can");
    }
}
