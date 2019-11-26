package com.mycompany.myapp.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.Properties;

@Configuration
@Profile("!test")
public class KafkaConfiguration {
    @Bean
    public AdminClient adminClient(KafkaProperties kafkaProperties
                                   //@Value("${kafka.bootstrap.servers}") String bootstrapServers
                                    ) {
        //Properties props = new Properties();
        //props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(kafkaProperties.getConsumerProps());
    }
}
