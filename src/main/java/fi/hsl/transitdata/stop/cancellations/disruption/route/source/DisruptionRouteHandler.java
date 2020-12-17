package fi.hsl.transitdata.stop.cancellations.disruption.route.source;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.stop.cancellations.db.DoiStopInfoSource;
import fi.hsl.transitdata.stop.cancellations.disruption.route.source.models.DisruptionRoute;
import fi.hsl.transitdata.stop.cancellations.models.Journey;
import fi.hsl.transitdata.stop.cancellations.models.JourneyPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class DisruptionRouteHandler {
    private static final Logger log = LoggerFactory.getLogger(DisruptionRouteHandler.class);

    final OmmDisruptionRouteSource disruptionRouteSource;
    final DoiAffectedJourneySource affectedJourneySource;
    final DoiAffectedJourneyPatternSource affectedJourneyPatternSource;

    public DisruptionRouteHandler(PulsarApplicationContext context, String ommConnString, String doiConnString, boolean useTestDoiQueries, boolean useTestOmmQueries) throws SQLException {
        disruptionRouteSource = OmmDisruptionRouteSource.newInstance(context, ommConnString, useTestOmmQueries);
        affectedJourneySource = DoiAffectedJourneySource.newInstance(context, doiConnString, useTestDoiQueries);
        affectedJourneyPatternSource = DoiAffectedJourneyPatternSource.newInstance(context, doiConnString, useTestDoiQueries);
    }

    public Optional<InternalMessages.StopCancellations> queryAndProcessResults(DoiStopInfoSource doiStops)  throws SQLException {
        List<DisruptionRoute> disruptionRoutes = disruptionRouteSource.queryAndProcessResults(doiStops.getDoiStopInfo());

        if (disruptionRoutes.isEmpty()) {
            return Optional.empty();
        }

        for (DisruptionRoute disruptionRoute : disruptionRoutes) {
            List<Journey> affectedJourneys = affectedJourneySource.getByDisruptionRoute(disruptionRoute);
            disruptionRoute.addAffectedJourneys(affectedJourneys);
        }

        // get unique journey pattern ids from affected journeys for querying affected journey patterns
        Set<String> affectedJourneyPatternIds = disruptionRoutes.stream()
                .map(DisruptionRoute::getAffectedJourneyPatternIds)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        if (affectedJourneyPatternIds.isEmpty()) {
            return Optional.empty();
        }

        Map<String, JourneyPattern> affectedJourneyPatternsById = affectedJourneyPatternSource.queryByJourneyPatternIds(affectedJourneyPatternIds);

        for (DisruptionRoute dr : disruptionRoutes) {
            dr.findAddAffectedStops(affectedJourneyPatternsById);
        }

        InternalMessages.StopCancellations message = getStopCancellationsProtoBuf(disruptionRoutes, affectedJourneyPatternsById);

        return Optional.of(message);
    }

    private InternalMessages.StopCancellations getStopCancellationsProtoBuf(List<DisruptionRoute> disruptionRoutes, Map<String, JourneyPattern> affectedJourneyPatternsById) {
        List<InternalMessages.StopCancellations.StopCancellation> stopCancellations = disruptionRoutes.stream()
                .map(DisruptionRoute::getAsStopCancellations)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        List<InternalMessages.JourneyPattern> affectedJourneyPatterns = disruptionRoutes.stream()
                .map(dr -> dr.getAffectedJourneyPatterns(affectedJourneyPatternsById))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        InternalMessages.StopCancellations.Builder builder = InternalMessages.StopCancellations.newBuilder();
        builder.addAllStopCancellations(stopCancellations);
        builder.addAllAffectedJourneyPatterns(affectedJourneyPatterns);
        return builder.build();
    }

}
