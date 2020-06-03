package fi.hsl.transitdata.stop.cancellations.disruption.route.source;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.stop.cancellations.db.QueryUtils;
import fi.hsl.transitdata.stop.cancellations.disruption.route.source.models.DisruptionRoute;
import fi.hsl.transitdata.stop.cancellations.models.Stop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OmmDisruptionRouteSource {

    private static final Logger log = LoggerFactory.getLogger(OmmDisruptionRouteSource.class);
    private final Connection dbConnection;
    private final String queryString;
    private final String timezone;

    private OmmDisruptionRouteSource(Connection connection, String timezone) {
        dbConnection = connection;
        queryString = QueryUtils.createQuery(getClass() ,"/disruption_routes.sql");
        this.timezone = timezone;
    }

    public static OmmDisruptionRouteSource newInstance(PulsarApplicationContext context, String jdbcConnectionString) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        final String timezone = context.getConfig().getString("omm.timezone");
        return new OmmDisruptionRouteSource(connection, timezone);
    }

    public List<DisruptionRoute> queryAndProcessResults(Map<String, Stop> stopsByGid) throws SQLException {
        log.info("Querying disruption routes from database");
        String dateFrom = QueryUtils.localDateAsString(Instant.now(), timezone);
        String preparedString = queryString.replace("VAR_DATE_FROM", dateFrom);

        try (PreparedStatement statement = dbConnection.prepareStatement(preparedString)) {
            ResultSet resultSet = statement.executeQuery();
            return parseDisruptionRoutes(resultSet, stopsByGid);
        }
        catch (Exception e) {
            log.error("Error while  querying and processing messages", e);
            throw e;
        }
    }

    private List<DisruptionRoute> parseDisruptionRoutes(ResultSet resultSet, Map<String, Stop> stopsByGid) throws SQLException {
        List<DisruptionRoute> disruptionRoutes = new ArrayList<>();
        log.info("Processing disruptionRoutes resultset");
        while (resultSet.next()) {
            try {
                String disruptionRouteId = resultSet.getString("DISRUPTION_ROUTES_ID");
                String startStopGid = resultSet.getString("START_STOP_ID");
                String startStopId = stopsByGid.containsKey(startStopGid) ? stopsByGid.get(startStopGid).stopId : "";
                String endStopGid = resultSet.getString("END_STOP_ID");
                String endStopId = stopsByGid.containsKey(startStopGid) ? stopsByGid.get(endStopGid).stopId : "";
                String affectedRoutes = resultSet.getString("AFFECTED_ROUTE_IDS");
                String validFrom = resultSet.getString("DC_VALID_FROM");
                String validTo = resultSet.getString("DC_VALID_TO");
                disruptionRoutes.add(new DisruptionRoute(disruptionRouteId, startStopId, endStopId, affectedRoutes, validFrom, validTo, timezone));
                String name = resultSet.getString("NAME");
                String description = resultSet.getString("DESCRIPTION");
                String type = resultSet.getString("DC_TYPE");
                log.info("Found disruption route with name: {}, description: {} and type: {}", name, description, type);
            } catch (IllegalArgumentException iae) {
                log.error("Error while parsing the disruptionRoutes resultset", iae);
            }
        }
        log.info("Found total {} disruption routes", disruptionRoutes.size());
        return disruptionRoutes;
    }
}
