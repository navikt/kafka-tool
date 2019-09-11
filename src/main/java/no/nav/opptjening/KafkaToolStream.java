package no.nav.opptjening;

import java.util.Collections;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Serialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import no.nav.opptjening.schema.skatt.hendelsesliste.Hendelse;
import no.nav.opptjening.schema.skatt.hendelsesliste.HendelseKey;

public class KafkaToolStream {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);
    private KafkaStreams stream;

    public KafkaToolStream(KafkaConfiguration kafkaConfiguration) {
        LOG.debug("Initializing aggregate count for topic: {}", KafkaConfiguration.SKATTEOPPGJØRHENDELSE_TOPIC);
        StreamsBuilder streamBuilder = new StreamsBuilder();
        KStream<HendelseKey, Hendelse> builder = streamBuilder.stream(KafkaConfiguration.SKATTEOPPGJØRHENDELSE_TOPIC);
        builder.flatMapValues((readOnlyKey, value) -> Collections.singletonList(readOnlyKey.getGjelderPeriode()))
                .selectKey((key, value) -> key.getGjelderPeriode())
                .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
                .count()
                .toStream()
                .foreach(this::logResult);
        stream = new KafkaStreams(streamBuilder.build(), kafkaConfiguration.streamsConfiguration());
        stream.setUncaughtExceptionHandler((t, e) -> {
            LOG.error("Uncaught exception in thread {}, closing stream", t, e);
            stream.close();
        });
        stream.setStateListener((newState, oldState) -> {
            LOG.debug("State change from {} to {}", oldState, newState);
        });
    }

    void logResult(String key, Long value) {
        LOG.debug("*********************************************");
        LOG.debug("year: {} count: {}", key, value);
        LOG.debug("*********************************************");
    }

    void start() {
        stream.start();
    }

    void close() {
        stream.close();
    }

    boolean isRunning() {
        return stream.state().isRunning();
    }
}
