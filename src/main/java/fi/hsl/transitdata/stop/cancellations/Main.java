package fi.hsl.transitdata.stop.cancellations;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.config.ConfigUtils;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.stop.cancellations.closed.stop.source.ClosedStopHandler;
import fi.hsl.transitdata.stop.cancellations.db.DoiStopInfoSource;
import fi.hsl.transitdata.stop.cancellations.disruption.journey.source.DisruptionJourneyHandler;
import fi.hsl.transitdata.stop.cancellations.disruption.route.source.DisruptionRouteHandler;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
            final DisruptionJourneyHandler disruptionJourneyHandler = new DisruptionJourneyHandler(context, doiConnString);
            final StopCancellationPublisher publisher = new StopCancellationPublisher(context);

            scheduler.scheduleAtFixedRate(() -> {
                try {
                    //Query and process stop cancellations by closed stops
                    final Optional<InternalMessages.StopCancellations> stopCancellationsClosed = closedStopHandler.queryAndProcessResults(doiStops);
                    //Query and process stop cancellations by disruption routes and affected journeys
                    final Optional<InternalMessages.StopCancellations> stopCancellationsJourneyPatternDetour = disruptionRouteHandler.queryAndProcessResults(doiStops);
                    // Query and process stop cancellations by disruption journeys
                    final Optional<InternalMessages.StopCancellations> stopCancellationsDisruptionJourney = disruptionJourneyHandler.queryAndProcessResults(doiStops);

                    if (stopCancellationsClosed.isPresent() || stopCancellationsJourneyPatternDetour.isPresent()) {
                        publisher.sendStopCancellations(mergeStopCancellations(unwrapOptionals(Arrays.asList(stopCancellationsClosed, stopCancellationsJourneyPatternDetour))));
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

    private static <T> Collection<T> unwrapOptionals(List<Optional<T>> optionals) {
        return optionals.stream().filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
    }

    private static InternalMessages.StopCancellations mergeStopCancellations(Collection<InternalMessages.StopCancellations> stopCancellationMessages) {
        InternalMessages.StopCancellations.Builder stopCancellationsBuilder = InternalMessages.StopCancellations.newBuilder();

        if (stopCancellationMessages.size() == 1) {
            return stopCancellationMessages.iterator().next();
        } else {
            //Merge journey patterns from all stop cancellation messages
            stopCancellationMessages.stream()
                    .map(InternalMessages.StopCancellations::getAffectedJourneyPatternsList)
                    .flatMap(List::stream)
                    .collect(Collectors.groupingBy(InternalMessages.JourneyPattern::getJourneyPatternId)) //since there may be many with same jpId
                    .values().stream()
                    .map(Main::combineTripsOfJourneyPatterns)
                    .forEach(stopCancellationsBuilder::addAffectedJourneyPatterns);

            //Collect stop cancellations from all stop cancellation messages
            stopCancellationMessages.stream()
                    .map(InternalMessages.StopCancellations::getStopCancellationsList)
                    .flatMap(List::stream)
                    .forEach(stopCancellationsBuilder::addStopCancellations);

            return stopCancellationsBuilder.build();
        }
    }

    // combines affected trips of one or more journeyPattern(s) by adding their unique trips to a single returned journeyPattern
    private static InternalMessages.JourneyPattern combineTripsOfJourneyPatterns(List<InternalMessages.JourneyPattern> repeatedJourneyPatterns) {
        if (repeatedJourneyPatterns.size() == 1) {
            return repeatedJourneyPatterns.iterator().next();
        } else {
            InternalMessages.JourneyPattern.Builder builder = repeatedJourneyPatterns.iterator().next().toBuilder();
            List <InternalMessages.TripInfo> uniqueTrips = repeatedJourneyPatterns.stream()
                    .map(InternalMessages.JourneyPattern::getTripsList)
                    .flatMap(List::stream)
                    .distinct()
                    .collect(Collectors.toList());
            builder.clearTrips().addAllTrips(uniqueTrips);
            return builder.build();
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
