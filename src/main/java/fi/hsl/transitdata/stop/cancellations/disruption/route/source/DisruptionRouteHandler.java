package fi.hsl.transitdata.stop.cancellations.disruption.route.source;

import fi.hsl.transitdata.stop.cancellations.disruption.route.source.models.DisruptionRoute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

public class DisruptionRouteHandler {

    private static final Logger log = LoggerFactory.getLogger(DisruptionRouteHandler.class);
    final OmmDisruptionRouteSource disruptionRouteSource;

    public DisruptionRouteHandler(String ommConnString) throws SQLException {
        disruptionRouteSource = OmmDisruptionRouteSource.newInstance(ommConnString);
    }

    public List<DisruptionRoute> queryAndProcessResults ()  throws SQLException {
        List<DisruptionRoute> disruptionRoutes = disruptionRouteSource.queryAndProcessResults();
        log.info("Processing {} disruption routes", disruptionRoutes.size());

        return disruptionRoutes;
    }

}
