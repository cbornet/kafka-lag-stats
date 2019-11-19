package com.mycompany.myapp.service;

import com.mycompany.myapp.config.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class KafkaKafkaConsumer implements InitializingBean, DisposableBean {

    private final Logger log = LoggerFactory.getLogger(KafkaKafkaConsumer.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public static final String TOPIC = "topic_kafka";

    private final KafkaProperties kafkaProperties;

    private KafkaConsumer<String, String> kafkaConsumer;

    public KafkaKafkaConsumer(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    public KafkaConsumer<String, String> getKafkaConsumer() {
        return kafkaConsumer;
    }

    @Override
    public void destroy() {
        log.info("Shutdown Kafka consumer");
        closed.set(true);
        kafkaConsumer.wakeup();
    }

    @Override
    public void afterPropertiesSet() {
        this.kafkaConsumer = new KafkaConsumer<>(kafkaProperties.getConsumerProps());

        Thread consumerThread = new Thread(() -> {
            try {
                kafkaConsumer.subscribe(Collections.singletonList(TOPIC));
                log.info("Kafka consumer started");
                while (!closed.get()) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
                    //log.info("Polled {} messages", records.count());

                    for (ConsumerRecord<String, String> record : records) {
                        //log.info("Consumed message in {} : {}", TOPIC, record.value());
                        Thread.sleep(100);
                    }
                    kafkaConsumer.commitSync();
                }
                kafkaConsumer.commitSync();
            } catch (WakeupException e) {
                // Ignore exception if closing
                if (!closed.get()) throw e;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                kafkaConsumer.close();
            }
        });
        consumerThread.start();
    }
}
