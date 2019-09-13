package no.nav.opptjening;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.streams.StreamsConfig;

import io.confluent.common.utils.TestUtils;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class KafkaConfiguration {
    static final String SKATTEOPPGJORHENDELSE_TOPIC = "privat-tortuga-skatteoppgjorhendelse";

    public static class Properties {
        static final String BOOTSTRAP_SERVERS = "KAFKA_BOOTSTRAP_SERVERS";
        static final String SCHEMA_REGISTRY_URL = "SCHEMA_REGISTRY_URL";
        static final String USERNAME = "KAFKA_USERNAME";
        static final String PASSWORD = "KAFKA_PASSWORD";
        static final String SASL_MECHANISM = "KAFKA_SASL_MECHANISM";
        static final String SECURITY_PROTOCOL = "KAFKA_SECURITY_PROTOCOL";
    }

    private final String bootstrapServers;
    private final String schemaUrl;
    private String securityProtocol;
    private String saslMechanism;
    private String saslJaasConfig;

    KafkaConfiguration(Map<String, String> env) {
        this.bootstrapServers = getFromEnvironment(env, Properties.BOOTSTRAP_SERVERS);
        this.schemaUrl = env.getOrDefault(Properties.SCHEMA_REGISTRY_URL, "http://kafka-schema-registry.tpa:8081");
        this.saslMechanism = env.getOrDefault(Properties.SASL_MECHANISM, "PLAIN");
        this.securityProtocol = env.getOrDefault(Properties.SECURITY_PROTOCOL, "SASL_SSL");
        this.saslJaasConfig = createPlainLoginModule(
                getFromEnvironment(env, Properties.USERNAME),
                getFromEnvironment(env, Properties.PASSWORD)
        );
    }

    private String createPlainLoginModule(String username, String password) {
        return "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";";
    }

    private Map<String, Object> getCommonConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        configs.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
        configs.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        return configs;
    }

    public java.util.Properties streamsConfiguration() {
        Map<String, Object> configs = getCommonConfigs();
        final java.util.Properties streamsConfiguration = new java.util.Properties();
        streamsConfiguration.putAll(configs);
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-tool-23q2gy525");
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return streamsConfiguration;
    }

    static String getFromEnvironment(Map<String, String> env, String propertyName) {
        return Optional.ofNullable(env.get(propertyName))
                .orElseThrow(() -> new RuntimeException(propertyName + " not found in environment"));
    }
}
