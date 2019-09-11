package no.nav.opptjening;

import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import no.nav.opptjening.schema.skatt.hendelsesliste.Hendelse;

class ApplicationTest {

    @BeforeAll
    static void beforeAll() {
        KafkaTestEnvironment.setup();
        new Application(KafkaTestEnvironment.getKafkaConfiguration()).start();
    }

    @Test
    void shoulPrint() {
        KafkaTestEnvironment.populateHendelseTopic(List.of(
                new Hendelse(1L, "1", "2017"),
                new Hendelse(7L, "2", "2017"),
                new Hendelse(50L, "1", "2018"),
                new Hendelse(133L, "4", "2019")
        ));
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}