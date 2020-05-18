package fi.hsl.transitdata.stop.cancellations.disruption.route.source;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.stop.cancellations.db.DoiStopInfoSource;
import fi.hsl.transitdata.stop.cancellations.disruption.route.source.models.DisruptionRoute;
import fi.hsl.transitdata.stop.cancellations.models.Journey;
import fi.hsl.transitdata.stop.cancellations.models.JourneyPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class DisruptionRouteHandler {

    private static final Logger log = LoggerFactory.getLogger(DisruptionRouteHandler.class);
    final OmmDisruptionRouteSource disruptionRouteSource;
    final DoiAffectedJourneySource affectedJourneySource;
    final DoiAffectedJourneyPatternSource affectedJourneyPatternSource;

    public DisruptionRouteHandler(PulsarApplicationContext context, String ommConnString, String doiConnString) throws SQLException {
        disruptionRouteSource = OmmDisruptionRouteSource.newInstance(ommConnString);
        affectedJourneySource = DoiAffectedJourneySource.newInstance(context, doiConnString);
        affectedJourneyPatternSource = DoiAffectedJourneyPatternSource.newInstance(context, doiConnString);
    }

    public Optional<List<DisruptionRoute>> queryAndProcessResults (DoiStopInfoSource doiStops)  throws SQLException {
        List<DisruptionRoute> disruptionRoutes = disruptionRouteSource.queryAndProcessResults(doiStops.getStopsByGidMap());

        if (disruptionRoutes.size() == 0) return Optional.empty();

        for (DisruptionRoute disruptionRoute : disruptionRoutes) {
            List<Journey> affectedJourneys = affectedJourneySource.getByDisruptionRoute(disruptionRoute);
            disruptionRoute.addAffectedJourneys(affectedJourneys);
        }

        // get unique journey pattern ids from affected journeys for querying affected journey patterns
        List<String> affectedJourneyPatternIds = disruptionRoutes.stream().map(DisruptionRoute::getAffectedJourneyPatternIds)
                .flatMap(List::stream)
                .distinct().collect(Collectors.toList());

        if (affectedJourneyPatternIds.size() == 0) return Optional.empty();

        Map<String, JourneyPattern> affectedJourneyPatternsById = affectedJourneyPatternSource.queryByJourneyPatternIds(affectedJourneyPatternIds);
        affectedJourneyPatternsById.values().forEach(JourneyPattern::orderStopsBySequence);

        for (DisruptionRoute dr : disruptionRoutes) {
            dr.findAddAffectedStops(affectedJourneyPatternsById);
        }

        //TODO create stop cancellations for affected stops considering the valid from/to times of the disruption routes

        //TODO make sure that it's okay to map stop cancellations (by disruption routes) to journey patterns
        //TODO change return type when needed
        return Optional.of(disruptionRoutes);
    }

}
