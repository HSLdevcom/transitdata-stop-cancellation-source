package fi.hsl.transitdata.stop.cancellations.disruption.journey.source;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.stop.cancellations.db.QueryUtils;
import fi.hsl.transitdata.stop.cancellations.disruption.journey.source.models.DisruptionJourney;
import fi.hsl.transitdata.stop.cancellations.models.Stop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DisruptionJourneysStopsSource {

    private static final Logger log = LoggerFactory.getLogger(DisruptionJourneysStopsSource.class);
    private final Connection dbConnection;
    private final String queryString;
    private final String timezone;
    private final int queryFutureInDays;

    private DisruptionJourneysStopsSource(Connection connection, String timezone, int queryFutureInDays) {
        dbConnection = connection;
        queryString = QueryUtils.createQuery(getClass() ,"/disruption_journeys_stops.sql");
        this.timezone = timezone;
        this.queryFutureInDays = queryFutureInDays;
    }

    public static DisruptionJourneysStopsSource newInstance(PulsarApplicationContext context, String jdbcConnectionString) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        final String timezone = context.getConfig().getString("omm.timezone");
        final int queryFutureInDays = context.getConfig().getInt("doi.queryFutureJourneysInDays");
        return new DisruptionJourneysStopsSource(connection, timezone, queryFutureInDays);
    }

    public List<DisruptionJourney> queryAndProcessResults(Map<String, Stop> stopsByGid) throws SQLException {
        log.info("Querying disruption journeys from database");
        String dateFrom = QueryUtils.localDateAsString(Instant.now(), timezone);
        String dateTo = QueryUtils.getOffsetDateAsString(Instant.now(), timezone, queryFutureInDays);

        String preparedString = queryString
                .replace("VAR_DATE_FROM", dateFrom)
                .replace("VAR_DATE_TO", dateTo);

        try (PreparedStatement statement = dbConnection.prepareStatement(preparedString)) {
            ResultSet resultSet = statement.executeQuery();
            return parseDisruptionJourneys(resultSet, stopsByGid);
        }
        catch (Exception e) {
            log.error("Error while querying and processing disruption routes", e);
            throw e;
        }
    }

    private List<DisruptionJourney> parseDisruptionJourneys(ResultSet resultSet, Map<String, Stop> stopsByGid) throws SQLException {
        List<DisruptionJourney> disruptionJourneys = new ArrayList<>();
        log.info("Processing disruptionJourneys resultset");
        while (resultSet.next()) {
            try {
                String stopGid = resultSet.getString("AFFECTED_STOPS_GID");
                String stopId = stopsByGid.containsKey(stopGid) ? stopsByGid.get(stopGid).stopId : "";
                String tripId = resultSet.getString("DVJ_Id");
                String operatingDay = resultSet.getString("OPERATING_DAY");
                String routeName = resultSet.getString("ROUTE_NAME");
                int direction = resultSet.getInt("DIRECTION");
                String startTime = resultSet.getString("START_TIME");
                String journeyPatternId = resultSet.getString("JP_Id");
                disruptionJourneys.add(new DisruptionJourney(tripId, operatingDay, routeName, direction, startTime, journeyPatternId, stopId));
            } catch (IllegalArgumentException iae) {
                log.error("Error while parsing the disruptionJourneys resultset", iae);
            }
        }
        log.info("Found total {} disruption journeys", disruptionJourneys.size());
        return disruptionJourneys;
    }
}
