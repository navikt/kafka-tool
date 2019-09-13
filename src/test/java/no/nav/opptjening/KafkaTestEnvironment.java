package no.nav.opptjening;

import static java.lang.Integer.MAX_VALUE;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import static no.nav.opptjening.KafkaConfiguration.SKATTEOPPGJORHENDELSE_TOPIC;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

import no.nav.common.KafkaEnvironment;
import no.nav.opptjening.schema.skatt.hendelsesliste.Hendelse;
import no.nav.opptjening.schema.skatt.hendelsesliste.HendelseKey;

class KafkaTestEnvironment {
    private static final String KAFKA_USERNAME = "srvTest";
    private static final String KAFKA_PASSWORD = "opensourcedPassword";
    private static final String TOPICS = SKATTEOPPGJORHENDELSE_TOPIC;
    private static final int NUMBER_OF_BROKERS = 3;

    private static Map<String, Object> configs;
    private static KafkaEnvironment kafkaEnvironment;

    static void setup() {
        kafkaEnvironment = new KafkaEnvironment(NUMBER_OF_BROKERS, List.of(TOPICS), emptyList(), true, false, emptyList(), false, new Properties());
        kafkaEnvironment.start();
        configs = getCommonConfig();
    }

    private static String getBrokersURL() {
        return kafkaEnvironment.getBrokersURL();
    }

    private static String getSchemaRegistryUrl() {
        return requireNonNull(kafkaEnvironment.getSchemaRegistry()).getUrl();
    }

    static KafkaConfiguration getKafkaConfiguration() {
        return new KafkaConfiguration(getTestEnvironment());
    }

    private static Map<String, String> getTestEnvironment() {
        Map<String, String> testEnvironment = new HashMap<>();
        testEnvironment.put("KAFKA_BOOTSTRAP_SERVERS", getBrokersURL());
        testEnvironment.put("SCHEMA_REGISTRY_URL", getSchemaRegistryUrl());
        testEnvironment.put("KAFKA_USERNAME", KAFKA_USERNAME);
        testEnvironment.put("KAFKA_PASSWORD", KAFKA_PASSWORD);
        testEnvironment.put("KAFKA_SASL_MECHANISM", "PLAIN");
        testEnvironment.put("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT");
        return testEnvironment;
    }

    private static Map<String, Object> getCommonConfig() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BOOTSTRAP_SERVERS_CONFIG, getBrokersURL());
        configs.put(SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryUrl());
        return configs;
    }

    private static KafkaProducer<HendelseKey, Hendelse> getKafkaProducer() {
        var producerConfig = new HashMap<>(configs);
        producerConfig.put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerConfig.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerConfig.put(ACKS_CONFIG, "all");
        producerConfig.put(RETRIES_CONFIG, MAX_VALUE);
        return new KafkaProducer<>(producerConfig);
    }

    private static HendelseKey getHendelseKey(Hendelse hendelse) {
        return HendelseKey.newBuilder()
                .setIdentifikator(hendelse.getIdentifikator())
                .setGjelderPeriode(hendelse.getGjelderPeriode()).build();
    }

    private static ProducerRecord<HendelseKey, Hendelse> createRecord(Hendelse hendelse) {
        return new ProducerRecord<>(TOPICS, getHendelseKey(hendelse), hendelse);
    }

    static void populateHendelseTopic(List<Hendelse> hendelser) {
        var producer = getKafkaProducer();
        hendelser.stream()
                .map(KafkaTestEnvironment::createRecord)
                .forEach(producer::send);
    }
}

