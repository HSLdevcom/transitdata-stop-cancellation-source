package fi.hsl.transitdata.omm.db;

import fi.hsl.transitdata.omm.models.DisruptionRoute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class OmmDisruptionRouteSource {

    private static final Logger log = LoggerFactory.getLogger(OmmDisruptionRouteSource.class);
    private final Connection dbConnection;
    private final String queryString;

    private OmmDisruptionRouteSource(Connection connection) {
        dbConnection = connection;
        queryString = QueryUtils.createQuery(getClass() ,"/disruption_routes.sql");
    }

    public static OmmDisruptionRouteSource newInstance(String jdbcConnectionString) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        return new OmmDisruptionRouteSource(connection);
    }

    public List<DisruptionRoute> queryAndProcessResults() throws SQLException {
        log.info("Querying disruption routes from database");
        try (PreparedStatement statement = dbConnection.prepareStatement(queryString)) {
            ResultSet resultSet = statement.executeQuery();
            return parseDisruptionRoutes(resultSet);
        }
        catch (Exception e) {
            log.error("Error while  querying and processing messages", e);
            throw e;
        }
    }

    private List<DisruptionRoute> parseDisruptionRoutes(ResultSet resultSet) throws SQLException {
        List<DisruptionRoute> disruptionRoutes = new ArrayList<>();
        log.info("Processing disruptionRoutes resultset");
        while (resultSet.next()) {
            try {
                String disruptionRouteId = resultSet.getString("DISRUPTION_ROUTES_ID");
                String startStopId = resultSet.getString("START_STOP_ID");
                String endStopId = resultSet.getString("END_STOP_ID");
                String affectedRoutes = resultSet.getString("AFFECTED_ROUTE_IDS");
                String validFrom = resultSet.getString("DC_VALID_FROM");
                String validTo = resultSet.getString("DC_VALID_TO");
                disruptionRoutes.add(new DisruptionRoute(disruptionRouteId, startStopId, endStopId, affectedRoutes, validFrom, validTo));
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
