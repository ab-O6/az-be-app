package com.example.az_be_app.servicebus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.azure.spring.cloud.service.listener.MessageListener;
import com.azure.spring.cloud.service.servicebus.consumer.ServiceBusErrorHandler;
import com.azure.spring.cloud.service.servicebus.consumer.ServiceBusRecordMessageListener;
import com.azure.spring.messaging.servicebus.core.ServiceBusProcessorFactory;
import com.azure.spring.messaging.servicebus.core.properties.ServiceBusContainerProperties;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

/**
 * Auto-configuration that instruments all Service Bus message listeners with
 * W3C trace context propagation.
 *
 * <p>
 * Works by decorating the {@link ServiceBusProcessorFactory} via a
 * {@link BeanPostProcessor}, so that every processor created through the
 * factory gets its {@code MessageListener} and {@code ServiceBusErrorHandler}
 * wrapped with trace-aware decorators. This ensures trace/span IDs appear
 * in the MDC for the entire message processing pipeline — including error
 * handling — with zero boilerplate in individual listener classes.
 * </p>
 *
 * <p>
 * Trace context is extracted from the {@code traceparent} or
 * {@code Diagnostic-Id} application property on the incoming
 * {@link ServiceBusReceivedMessage}, parsed as a W3C traceparent, and
 * restored as a remote parent span. A child {@link Observation} is then
 * created so that Micrometer's OTel bridge populates the SLF4J MDC.
 * </p>
 */
@Configuration(proxyBeanMethods = false)
public class ServiceBusTracingAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ServiceBusTracingAutoConfiguration.class);

    /**
     * Static {@link BeanPostProcessor} that wraps every
     * {@link ServiceBusProcessorFactory} bean with a tracing decorator.
     * Declared {@code static} so it is instantiated early, before other beans.
     */
    @Bean
    static BeanPostProcessor serviceBusTracingPostProcessor(
            ObjectProvider<ObservationRegistry> registryProvider) {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String beanName) {
                if (bean instanceof ServiceBusProcessorFactory factory) {
                    log.info("Wrapping ServiceBusProcessorFactory [{}] with trace context propagation", beanName);
                    return new TracingProcessorFactory(factory, registryProvider);
                }
                return bean;
            }
        };
    }

    // -----------------------------------------------------------------------
    // Tracing Processor Factory — decorates every createProcessor() call
    // -----------------------------------------------------------------------

    private static class TracingProcessorFactory implements ServiceBusProcessorFactory {

        private final ServiceBusProcessorFactory delegate;
        private final ObjectProvider<ObservationRegistry> registryProvider;

        TracingProcessorFactory(ServiceBusProcessorFactory delegate,
                ObjectProvider<ObservationRegistry> registryProvider) {
            this.delegate = delegate;
            this.registryProvider = registryProvider;
        }

        @Override
        public ServiceBusProcessorClient createProcessor(String topic,
                MessageListener<?> listener, ServiceBusErrorHandler errorHandler) {
            var holder = new TraceContextHolder();
            return delegate.createProcessor(topic,
                    listener != null ? wrapListener(listener, holder, topic) : null,
                    errorHandler != null ? wrapErrorHandler(errorHandler, holder) : null);
        }

        @Override
        public ServiceBusProcessorClient createProcessor(String topic,
                ServiceBusContainerProperties properties) {
            wrapContainerProperties(properties, topic);
            return delegate.createProcessor(topic, properties);
        }

        @Override
        public ServiceBusProcessorClient createProcessor(String topic, String subscription,
                MessageListener<?> listener, ServiceBusErrorHandler errorHandler) {
            var holder = new TraceContextHolder();
            return delegate.createProcessor(topic, subscription,
                    listener != null ? wrapListener(listener, holder, topic) : null,
                    errorHandler != null ? wrapErrorHandler(errorHandler, holder) : null);
        }

        @Override
        public ServiceBusProcessorClient createProcessor(String topic, String subscription,
                ServiceBusContainerProperties properties) {
            wrapContainerProperties(properties, topic);
            return delegate.createProcessor(topic, subscription, properties);
        }

        @Override
        public void addListener(Listener listener) {
            delegate.addListener(listener);
        }

        @Override
        public boolean removeListener(Listener listener) {
            return delegate.removeListener(listener);
        }

        // --- Container properties wrapping ---

        @SuppressWarnings({ "rawtypes" })
        private void wrapContainerProperties(ServiceBusContainerProperties properties, String topic) {
            var holder = new TraceContextHolder();
            MessageListener originalListener = properties.getMessageListener();
            ServiceBusErrorHandler originalErrorHandler = properties.getErrorHandler();

            if (originalListener != null) {
                properties.setMessageListener(wrapListener(originalListener, holder, topic));
            }
            if (originalErrorHandler != null) {
                properties.setErrorHandler(wrapErrorHandler(originalErrorHandler, holder));
            }
        }

        // --- Message listener wrapping ---

        /**
         * Wraps a {@link MessageListener} to extract the W3C traceparent from the
         * incoming message, restore the OTel remote context, and execute the
         * original listener inside a Micrometer {@link Observation}.
         *
         * <p>
         * On success, the {@link TraceContextHolder} is cleaned up. On failure,
         * it is intentionally left populated so the error handler (called by the
         * Azure SDK on the same {@code receiverPump} thread) can pick it up.
         * </p>
         */
        private ServiceBusRecordMessageListener wrapListener(MessageListener<?> delegate,
                TraceContextHolder holder,
                String destination) {
            return new TracingRecordMessageListener(delegate, holder, destination, registryProvider);
        }

        // --- Error handler wrapping ---

        /**
         * Wraps a {@link ServiceBusErrorHandler} to restore trace context from
         * the {@link TraceContextHolder} (populated by the message listener
         * decorator on the same thread) and execute the original handler inside
         * a Micrometer {@link Observation}.
         *
         * <p>
         * If no trace context is available (e.g. connection-level errors not tied
         * to a specific message), the original handler is called directly.
         * </p>
         */
        private ServiceBusErrorHandler wrapErrorHandler(ServiceBusErrorHandler delegate,
                TraceContextHolder holder) {
            return (errorContext) -> {
                Context remoteContext = holder.get();
                if (remoteContext != null) {
                    ObservationRegistry registry = registryProvider.getObject();
                    try (Scope scope = remoteContext.makeCurrent()) {
                        Observation.createNotStarted("servicebus.process.error", registry)
                                .lowCardinalityKeyValue("messaging.system", "servicebus")
                                .lowCardinalityKeyValue("messaging.destination.entity",
                                        errorContext.getEntityPath())
                                .observe(() -> delegate.accept(errorContext));
                    } finally {
                        holder.remove();
                    }
                } else {
                    delegate.accept(errorContext);
                }
            };
        }

    }

    // -----------------------------------------------------------------------
    // Tracing Record Message Listener — implements ServiceBusRecordMessageListener
    // -----------------------------------------------------------------------

    /**
     * Message listener decorator that explicitly implements
     * {@link ServiceBusRecordMessageListener} as required by Azure Spring Cloud's
     * {@code ServiceBusProcessorClientBuilderFactory}.
     */
    private static class TracingRecordMessageListener implements ServiceBusRecordMessageListener {

        private final MessageListener<?> delegate;
        private final TraceContextHolder holder;
        private final String destination;
        private final ObjectProvider<ObservationRegistry> registryProvider;

        TracingRecordMessageListener(MessageListener<?> delegate,
                TraceContextHolder holder,
                String destination,
                ObjectProvider<ObservationRegistry> registryProvider) {
            this.delegate = delegate;
            this.holder = holder;
            this.destination = destination;
            this.registryProvider = registryProvider;
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public void onMessage(ServiceBusReceivedMessageContext msgCtx) {
            ServiceBusReceivedMessage sbMessage = msgCtx != null ? msgCtx.getMessage() : null;
            String traceparent = extractTraceparent(sbMessage);

            Context remoteContext = parseTraceparent(traceparent);
            holder.set(remoteContext);

            ObservationRegistry registry = registryProvider.getObject();

            // Make the remote context current BEFORE starting the Observation so the
            // OTel bridge sees the remote parent when it creates the child span.
            try (@SuppressWarnings("unused")
            Scope scope = remoteContext.makeCurrent()) {
                Observation.createNotStarted("servicebus.process", registry)
                        .lowCardinalityKeyValue("messaging.system", "servicebus")
                        .lowCardinalityKeyValue("messaging.destination", destination)
                        .observe(() -> {
                            holder.set(Context.current());
                            ((MessageListener) delegate).onMessage(msgCtx);
                        });
            }
            // Only reached on success — clean up the holder.
            // On failure this line is skipped, leaving the context for the error handler.
            holder.remove();
        }
    }

    // -----------------------------------------------------------------------
    // Trace context holder — ThreadLocal bridge between listener and error handler
    // -----------------------------------------------------------------------

    /**
     * Per-container {@link ThreadLocal} holder that bridges trace context from
     * message processing to error handling on the same {@code receiverPump} thread.
     * Each container gets its own instance, so concurrent containers cannot
     * interfere with each other.
     */
    private static class TraceContextHolder {
        private final ThreadLocal<Context> contextHolder = new ThreadLocal<>();

        void set(Context context) {
            contextHolder.set(context);
        }

        Context get() {
            return contextHolder.get();
        }

        void remove() {
            contextHolder.remove();
        }
    }

    // -----------------------------------------------------------------------
    // Helpers & W3C traceparent parser
    // -----------------------------------------------------------------------

    static String extractTraceparent(ServiceBusReceivedMessage message) {
        if (message == null) {
            return null;
        }
        var props = message.getApplicationProperties();
        Object value = props.getOrDefault("traceparent",
                props.getOrDefault("Diagnostic-Id", null));
        return value instanceof String s ? s : null;
    }

    /**
     * Parses a W3C {@code traceparent} string
     * (e.g. {@code "00-7f8e9d0c1b2a3f4e5d6c7b8a9f0e1d2c-abcdef1234567890-01"})
     * into an OTel {@link Context} containing a remote {@link SpanContext}.
     *
     * @return a context with the remote span, or {@link Context#current()} if
     *         the input is null, blank, or malformed
     */
    static Context parseTraceparent(String traceparent) {
        if (traceparent == null || traceparent.isBlank()) {
            return Context.current();
        }
        // Format: version-traceId-parentId-traceFlags
        String[] parts = traceparent.trim().split("-");
        if (parts.length < 4) {
            log.warn("Invalid traceparent format: {}", traceparent);
            return Context.current();
        }
        try {
            SpanContext spanContext = SpanContext.createFromRemoteParent(
                    parts[1], parts[2],
                    TraceFlags.fromHex(parts[3], 0),
                    TraceState.getDefault());
            if (!spanContext.isValid()) {
                log.warn("Invalid span context parsed from traceparent: {}", traceparent);
                return Context.current();
            }
            return Context.current().with(Span.wrap(spanContext));
        } catch (Exception e) {
            log.warn("Failed to parse traceparent '{}': {}", traceparent, e.getMessage());
            return Context.current();
        }
    }
}
