package fi.hsl.transitdata.stop.cancellations.disruption.route.source;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.stop.cancellations.db.QueryUtils;
import fi.hsl.transitdata.stop.cancellations.disruption.route.source.models.DisruptionRoute;
import fi.hsl.transitdata.stop.cancellations.models.Journey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.*;

public class DoiAffectedJourneySource {

    private static final Logger log = LoggerFactory.getLogger(DoiAffectedJourneySource.class);
    private final Connection dbConnection;
    private final String queryString;
    private final String timeZone;
    private final int queryFutureInDays;

    private DoiAffectedJourneySource(PulsarApplicationContext context, Connection connection, boolean useTestDoiQueries) {
        dbConnection = connection;
        queryString = QueryUtils.createQuery(getClass(),useTestDoiQueries ? "/affected_journeys_by_disruption_routes_test.sql" : "/affected_journeys_by_disruption_routes.sql");
        timeZone = context.getConfig().getString("omm.timezone");
        queryFutureInDays = context.getConfig().getInt("doi.queryFutureJourneysInDays");
        log.info("Using {} future days in querying affected journeys", queryFutureInDays);
    }

    public static DoiAffectedJourneySource newInstance(PulsarApplicationContext context, String jdbcConnectionString, boolean useTestDoiQueries) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        return new DoiAffectedJourneySource(context, connection, useTestDoiQueries);
    }

    public List<Journey> getByDisruptionRoute(DisruptionRoute disruptionRoute) throws SQLException {
        log.info("Querying affected journeys by disruption routes from database");
        String dateFrom = QueryUtils.localDateAsString(Instant.now(), timeZone);
        String dateTo = QueryUtils.getOffsetDateAsString(Instant.now(), timeZone, queryFutureInDays);
        String preparedString = queryString
                .replace("VAR_AFFECTED_ROUTE_IDS", String.join(",", disruptionRoute.affectedRoutes))
                .replace("VAR_DATE_FROM", dateFrom)
                .replace("VAR_DATE_TO", dateTo)
                .replace("VAR_MIN_DEP_TIME", disruptionRoute.getValidFrom().orElse(dateFrom))
                .replace("VAR_MAX_DEP_TIME",  disruptionRoute.getValidTo().orElse(dateTo));
        try (PreparedStatement statement = dbConnection.prepareStatement(preparedString)) {
            ResultSet resultSet = statement.executeQuery();
            return parseJourneys(resultSet);
        }
        catch (Exception e) {
            log.error("Error while  querying and processing affected journeys by disruption routes", e);
            throw e;
        }
    }

    private List<Journey> parseJourneys(ResultSet resultSet) throws SQLException {
        log.info("Processing affected journeys by disruption routes resultset");
        List<Journey> journeys = new ArrayList<>();
        while (resultSet.next()) {
            try {
                String tripId = resultSet.getString("DVJ_Id");
                String operatingDay = resultSet.getString("OPERATING_DAY");
                String routeName = resultSet.getString("ROUTE_NAME");
                int direction = resultSet.getInt("DIRECTION");
                String startTime = resultSet.getString("START_TIME");
                String journeyPatternId = resultSet.getString("JP_Id");
                Journey affectedJourney = new Journey(tripId, operatingDay, routeName, direction, startTime, journeyPatternId);
                journeys.add(affectedJourney);
            } catch (IllegalArgumentException iae) {
                log.error("Error while parsing affected journeys resultset", iae);
            }
        }
        log.info("Found {} affected journeys for disruption route", journeys.size());
        return journeys;
    }
}
