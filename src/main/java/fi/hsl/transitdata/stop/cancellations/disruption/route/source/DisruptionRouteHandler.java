package fi.hsl.transitdata.stop.cancellations.disruption.route.source;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.stop.cancellations.disruption.route.source.models.DisruptionRoute;
import fi.hsl.transitdata.stop.cancellations.models.Journey;
import fi.hsl.transitdata.stop.cancellations.models.JourneyPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    public List<DisruptionRoute> queryAndProcessResults ()  throws SQLException {
        List<DisruptionRoute> disruptionRoutes = disruptionRouteSource.queryAndProcessResults();
        log.info("Processing {} disruption routes", disruptionRoutes.size());

        for (DisruptionRoute route : disruptionRoutes) {
            List<Journey> affectedJourneys = affectedJourneySource.getByDisruptionRoute(route);
            route.addAffectedJourneys(affectedJourneys);
        }

        List<String> affectedJourneyPatternIds = new ArrayList<>();
        for (DisruptionRoute route : disruptionRoutes) {
            affectedJourneyPatternIds.addAll(route.getAffectedJourneyPatternIds());
        }
        affectedJourneyPatternIds = affectedJourneyPatternIds.stream().distinct().collect(Collectors.toList());

        Map<String, JourneyPattern> affectedJourneyPatterns = affectedJourneyPatternSource.queryByJourneyPatternIds(affectedJourneyPatternIds);

        return disruptionRoutes;
    }

}
