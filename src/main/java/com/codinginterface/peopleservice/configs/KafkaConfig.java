package com.codinginterface.peopleservice.configs;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Configuration
public class KafkaConfig {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    String bootstrapServers;

    @Value("${topics.people-basic.name}")
    String topicName;

    @Value("${topics.people-basic.replicas}")
    int topicReplicas;

    @Value("${topics.people-basic.partitions}")
    int topicPartitions;

    @Bean
    public NewTopic peopleBasicTopic() {
        return TopicBuilder.name(topicName)
                .partitions(topicPartitions)
                .replicas(topicReplicas)
                .build();
    }

    @Bean
    public NewTopic peopleBasicShortTopic() {
        return TopicBuilder.name(topicName + "-short")
                .partitions(topicPartitions)
                .replicas(topicReplicas)
                .config(TopicConfig.RETENTION_MS_CONFIG, "360000")
                .build();
    }

    @PostConstruct
    public void changePeopleBasicTopicRetention() {
        // create a connection with configs to bootstrap servers
        Map<String, Object> connectionConfigs = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
        );

        try (var admin = AdminClient.create(connectionConfigs)) {
            var configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

            if (admin.listTopics().names().get().contains(topicName)) {
                ConfigEntry topicConfigEntry = admin.describeConfigs(Collections.singleton(configResource))
                        .all().get().values().stream()
                        .flatMap(config -> config.entries().stream())
                        .filter(ce -> ce.name().equals(TopicConfig.RETENTION_MS_CONFIG))
                        .findFirst().orElse(null);

                if (topicConfigEntry != null && !topicConfigEntry.value().equals("360000")) {
                    var alterConfigEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "360000");
                    var alterOp = new AlterConfigOp(alterConfigEntry, AlterConfigOp.OpType.SET);

                    Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = Map.of(
                            configResource, Collections.singleton(alterOp)
                    );
                    admin.incrementalAlterConfigs(alterConfigs).all().get();
                    logger.info("Updated topic retention for " + topicName);
                }

            } else {
                logger.warn("Topic " + topicName + " does not exist");
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted");
        } catch (ExecutionException e) {
            logger.error("Execution exception occurred", e);
        }
    }
}


