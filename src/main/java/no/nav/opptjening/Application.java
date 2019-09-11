package no.nav.opptjening;

import static java.lang.System.exit;
import static java.lang.System.getenv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import no.nav.opptjening.nais.NaisHttpServer;

public class Application {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);
    ;
    private final KafkaToolStream stream;

    public static void main(String[] args) {
        try {
            Application application = new Application(new KafkaConfiguration(getenv()));
            application.start();

            NaisHttpServer naisHttpServer = new NaisHttpServer(application::isRunning, () -> true);
            naisHttpServer.start();
            addShutdownHook(naisHttpServer);
        } catch (Exception e) {
            LOG.error("Application failed to start", e);
            exit(1);
        }
    }

    public Application(KafkaConfiguration kafkaConfiguration) {
        stream = new KafkaToolStream(kafkaConfiguration);
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    void start() {
        stream.start();
    }

    void close() {
        stream.close();
    }

    private boolean isRunning() {
        return stream.isRunning();
    }

    private static void addShutdownHook(NaisHttpServer naisHttpServer) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                LOG.info("stopping nais http server");
                naisHttpServer.stop();
            } catch (Exception e) {
                LOG.error("Error while shutting down nais http server", e);
            }
        }));
    }
}
