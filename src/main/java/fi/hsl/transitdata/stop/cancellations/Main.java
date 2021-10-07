package fi.hsl.transitdata.stop.cancellations;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.config.ConfigUtils;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.stop.cancellations.closed.stop.source.ClosedStopHandler;
import fi.hsl.transitdata.stop.cancellations.db.DoiStopInfoSource;
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

        PulsarApplication app = null;

        try {
            final Config config = ConfigParser.createConfig();

            final String connString = readConnString("FILEPATH_CONNECTION_STRING", "TRANSITDATA_PUBTRANS_CAT_CONN_STRING");

            final boolean closedStopsEnabled = config.getBoolean("application.closedStopsEnabled");
            final boolean disruptionRouteEnabled = config.getBoolean("application.disruptionRouteEnabled");

            final boolean useTestDoiQueries = config.getBoolean("doi.useTestDbQueries");
            final boolean useTestOmmQueries = config.getBoolean("omm.useTestDbQueries");

            final int pollIntervalInSeconds = config.getInt("omm.interval");

            app = PulsarApplication.newInstance(config);
            final PulsarApplicationContext context = app.getContext();

            final DoiStopInfoSource doiStops = DoiStopInfoSource.newInstance(context, connString, useTestDoiQueries);

            final ClosedStopHandler closedStopHandler = new ClosedStopHandler(context, connString, connString, useTestDoiQueries, useTestOmmQueries);
            final DisruptionRouteHandler disruptionRouteHandler = new DisruptionRouteHandler(context, connString, connString, useTestDoiQueries, useTestOmmQueries);

            final StopCancellationPublisher publisher = new StopCancellationPublisher(context);

            PulsarApplication finalApp = app;
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    //Query closed stops, affected journey patterns and affected journeys
                    final Optional<InternalMessages.StopCancellations> stopCancellationsClosed = closedStopsEnabled ?
                            closedStopHandler.queryAndProcessResults(doiStops) :
                            Optional.empty();
                    
                    //Query disruption routes and affected journeys
                    final Optional<InternalMessages.StopCancellations> stopCancellationsJourneyPatternDetour = disruptionRouteEnabled ?
                            disruptionRouteHandler.queryAndProcessResults(doiStops) :
                            Optional.empty();

                    //Stop cancellation message should be sent even if there are no cancellations so that cancellation-of-cancellation works in the processor
                    publisher.sendStopCancellations(mergeStopCancellations(unwrapOptionals(Arrays.asList(stopCancellationsClosed, stopCancellationsJourneyPatternDetour))));
                } catch (PulsarClientException e) {
                    log.error("Pulsar connection error", e);
                    closeApplication(finalApp, scheduler);
                } catch (SQLException e) {
                    log.error("SQL exception", e);
                    closeApplication(finalApp, scheduler);
                } catch (Exception e) {
                    log.error("Unknown exception at poll cycle: ", e);
                    closeApplication(finalApp, scheduler);
                }
            }, 0, pollIntervalInSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Exception at Main: " + e.getMessage(), e);
            closeApplication(app, scheduler);
        }
    }

    private static <T> Collection<T> unwrapOptionals(List<Optional<T>> optionals) {
        return optionals.stream().filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
    }

    private static InternalMessages.StopCancellations mergeStopCancellations(Collection<InternalMessages.StopCancellations> stopCancellationMessages) {
        InternalMessages.StopCancellations.Builder stopCancellationsBuilder = InternalMessages.StopCancellations.newBuilder();

        //Merge journey patterns from all stop cancellation messages
        final Collection<InternalMessages.JourneyPattern> combinedJourneyPatterns = combineTripsOfJourneyPatterns(stopCancellationMessages.stream()
                .map(InternalMessages.StopCancellations::getAffectedJourneyPatternsList)
                .flatMap(List::stream)
                //There may be multiple journey patterns with same ID, but they all should include same trips
                .collect(Collectors.groupingBy(InternalMessages.JourneyPattern::getJourneyPatternId)));

        combinedJourneyPatterns.forEach(stopCancellationsBuilder::addAffectedJourneyPatterns);

        //Collect stop cancellations from all stop cancellation messages
        stopCancellationMessages.stream()
                .map(InternalMessages.StopCancellations::getStopCancellationsList)
                .flatMap(List::stream)
                .forEach(stopCancellationsBuilder::addStopCancellations);

        return stopCancellationsBuilder.build();
    }

    // combines affected trips of one or more journeyPattern(s) by adding their unique trips to a single returned journeyPattern
    //TODO: simplify SQL queries so that this would not be necessary
    private static Collection<InternalMessages.JourneyPattern> combineTripsOfJourneyPatterns(Map<String, List<InternalMessages.JourneyPattern>> journeyPatternById) {
        return journeyPatternById.entrySet().stream().map(journeyPatternsWithId -> {
            InternalMessages.JourneyPattern.Builder journeyPatternBuilder = InternalMessages.JourneyPattern.newBuilder();
            journeyPatternBuilder.setJourneyPatternId(journeyPatternsWithId.getKey());
            //Add list of stops from the first journey pattern
            journeyPatternBuilder.addAllStops(journeyPatternsWithId.getValue().get(0).getStopsList());
            //Get set of trips from all journey patterns
            journeyPatternBuilder.addAllTrips(journeyPatternsWithId.getValue().stream()
                    .flatMap(journeyPattern -> journeyPattern.getTripsList().stream())
                    .collect(Collectors.toSet())
            );

            return journeyPatternBuilder.build();
        }).collect(Collectors.toList());
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
        //Default path is what works with Docker out-of-the-box. Override with a local file if needed
        final String secretFilePath = ConfigUtils.getEnv(envVar)
                .orElse("/run/secrets/" + secretName);

        String connectionString = "";
        try (Scanner scanner = new Scanner(new File(secretFilePath)).useDelimiter("\\Z")) {
            connectionString = scanner.next();
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
