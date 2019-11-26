package com.mycompany.myapp.web.rest;

import com.mycompany.myapp.KafkaApp;
import com.mycompany.myapp.config.KafkaProperties;
import com.mycompany.myapp.service.KafkaKafkaProducer;
import com.mycompany.myapp.service.KafkaKafkaConsumer;
import com.mycompany.myapp.service.lag.ConsumerOffsetsReader;
import com.mycompany.myapp.service.lag.KafkaLagService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testcontainers.containers.KafkaContainer;

import java.time.Clock;
import java.util.Map;
import java.util.Properties;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = KafkaApp.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class KafkaKafkaResourceIT {

    private MockMvc restMockMvc;

    private static boolean started = false;

    private static KafkaContainer kafkaContainer;

    @Autowired
    private KafkaKafkaProducer producer;

    @Autowired
    private KafkaKafkaConsumer consumer;

    private static final int MAX_ATTEMPT = 5;

    @Autowired
    private Clock clock;

    @Autowired
    private KafkaProperties kafkaProperties;


    @BeforeAll
    public static void startServer() {
        if (!started) {
            startTestcontainer();
            started = true;
        }
    }

    private static void startTestcontainer() {
        kafkaContainer = new KafkaContainer("5.3.1");
        kafkaContainer.start();
        System.setProperty("kafkaBootstrapServers", kafkaContainer.getBootstrapServers());
    }

    @BeforeEach
    public void setup() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        AdminClient client = AdminClient.create(props);
        ConsumerOffsetsReader consumerOffsetsReader = new ConsumerOffsetsReader(client);
        KafkaLagService lagService = new KafkaLagService(kafkaProperties, consumerOffsetsReader, client);
        KafkaKafkaResource kafkaResource = new KafkaKafkaResource(producer, lagService, clock);

        this.restMockMvc = MockMvcBuilders.standaloneSetup(kafkaResource)
            .build();

        producer.init();
        consumer.start();
    }

    @Test
    public void producedMessageHasBeenConsumed() throws Exception {
        restMockMvc.perform(post("/api/kafka-kafka/publish?message=test"))
            .andExpect(status().isOk());

        Map<MetricName, ? extends Metric> metrics = consumer.getKafkaConsumer().metrics();

        Metric recordsConsumedTotalMetric = metrics.entrySet().stream()
            .filter(entry -> "records-consumed-total".equals(entry.getKey().name()))
            .findFirst()
            .get()
            .getValue();

        Double expectedTotalConsumedMessage = 1.0;
        Double totalConsumedMessage;
        int attempt = 0;
        do {
            totalConsumedMessage = (Double) recordsConsumedTotalMetric.metricValue();
            Thread.sleep(200);
        } while (!totalConsumedMessage.equals(expectedTotalConsumedMessage) && attempt++ < MAX_ATTEMPT);

        Assertions.assertThat(attempt).isLessThan(MAX_ATTEMPT);
        Assertions.assertThat(totalConsumedMessage).isEqualTo(expectedTotalConsumedMessage);
    }

}

