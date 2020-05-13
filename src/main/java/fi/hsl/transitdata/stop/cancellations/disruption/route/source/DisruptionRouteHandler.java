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

    public List<DisruptionRoute> queryAndProcessResults (DoiStopInfoSource doiStops)  throws SQLException {
        List<DisruptionRoute> disruptionRoutes = disruptionRouteSource.queryAndProcessResults();
        log.info("Processing {} disruption routes", disruptionRoutes.size());

        for (DisruptionRoute disruptionRoute : disruptionRoutes) {
            List<Journey> affectedJourneys = affectedJourneySource.getByDisruptionRoute(disruptionRoute);
            disruptionRoute.addAffectedJourneys(affectedJourneys);
        }

        List<String> affectedJourneyPatternIds = disruptionRoutes.stream().map(DisruptionRoute::getAffectedJourneyPatternIds)
                .flatMap(List::stream)
                .distinct().collect(Collectors.toList());

        Map<String, JourneyPattern> affectedJourneyPatterns = affectedJourneyPatternSource.queryByJourneyPatternIds(affectedJourneyPatternIds);

        //TODO make sure that stops of the affected journey patterns are in the right order
        //TODO find stops that are cancelled based on start & end stops of disruption routes (from affected journey patterns) (get matching stopIds from DoiStopInfoSource doiStops)
        //TODO create stop cancellations for affected stops considering the valid from/to times of the disruption routes

        //TODO make sure that it's okay to map stop cancellations (by disruption routes) to journey patterns
        //TODO change return type when needed
        return disruptionRoutes;
    }

}
