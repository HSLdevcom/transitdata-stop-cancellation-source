package fi.hsl.transitdata.stop.cancellations;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.config.ConfigUtils;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.stop.cancellations.closed.stop.source.ClosedStopHandler;
import fi.hsl.transitdata.stop.cancellations.disruption.route.source.DisruptionRouteHandler;
import fi.hsl.transitdata.stop.cancellations.db.DoiStopInfoSource;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        try {
            final Config config = ConfigParser.createConfig();
            final String doiConnString = readConnString("FILEPATH_CONNECTION_STRING", "TRANSITDATA_PUBTRANS_CONN_STRING");
            final String ommConnString = readConnString("FILEPATH_CONNECTION_STRING_TEST", "TRANSITDATA_PUBTRANS_TEST_CONN_STRING");
            final int pollIntervalInSeconds = config.getInt("omm.interval");
            final PulsarApplication app = PulsarApplication.newInstance(config);
            final PulsarApplicationContext context = app.getContext();

            final DoiStopInfoSource doiStops = DoiStopInfoSource.newInstance(context, doiConnString);
            final ClosedStopHandler closedStopHandler = new ClosedStopHandler(context, ommConnString, doiConnString);
            final DisruptionRouteHandler disruptionRouteHandler = new DisruptionRouteHandler(context, ommConnString, doiConnString);
            final StopCancellationPublisher publisher = new StopCancellationPublisher(context);

            scheduler.scheduleAtFixedRate(() -> {
                try {
                    //Query closed stops, affected journey patterns and affected journeys
                     Optional<InternalMessages.StopCancellations> stopCancellationsClosed = closedStopHandler.queryAndProcessResults(doiStops);
                    //Query disruption routes and affected journeys
                    Optional<InternalMessages.StopCancellations> stopCancellationsJourneyPatternDetour = disruptionRouteHandler.queryAndProcessResults(doiStops);
                    //TODO combine stop cancellations from closedStopHandler and disruptionRouteHandler

                    if (stopCancellationsJourneyPatternDetour.isPresent()) {
                        publisher.sendStopCancellations(stopCancellationsJourneyPatternDetour.get());
                    }
                } catch (PulsarClientException e) {
                    log.error("Pulsar connection error", e);
                    closeApplication(app, scheduler);
                } catch (SQLException e) {
                    log.error("SQL exception", e);
                    closeApplication(app, scheduler);
                } catch (Exception e) {
                    log.error("Unknown exception at poll cycle: ", e);
                    closeApplication(app, scheduler);
                }
            }, 0, pollIntervalInSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Exception at Main: " + e.getMessage(), e);
        }
    }

    private static void closeApplication(PulsarApplication app, ScheduledExecutorService scheduler) {
        log.warn("Closing application");
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
        }
        if (app != null) {
            app.close();
        }
    }

    private static String readConnString(String envVar, String secretName) throws Exception {
        String connectionString = "";
        try {
            //Default path is what works with Docker out-of-the-box. Override with a local file if needed
            final String secretFilePath = ConfigUtils.getEnv(envVar)
                                                     .orElse("/run/secrets/" + secretName);
            connectionString = new Scanner(new File(secretFilePath))
                    .useDelimiter("\\Z").next();
        } catch (Exception e) {
            log.error("Failed to read DB connection string from secrets", e);
            throw e;
        }

        if (connectionString.isEmpty()) {
            throw new Exception("Failed to find DB connection string, exiting application");
        }
        return connectionString;
    }
}
