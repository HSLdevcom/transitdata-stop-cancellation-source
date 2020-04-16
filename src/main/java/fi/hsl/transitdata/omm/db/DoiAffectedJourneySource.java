package fi.hsl.transitdata.omm.db;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.omm.models.AffectedJourney;
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

    private DoiAffectedJourneySource(PulsarApplicationContext context, Connection connection) {
        dbConnection = connection;
        queryString = QueryUtils.createQuery(getClass(),"/affected_journeys.sql");
        timeZone = context.getConfig().getString("omm.timezone");
    }

    public static DoiAffectedJourneySource newInstance(PulsarApplicationContext context, String jdbcConnectionString) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        return new DoiAffectedJourneySource(context, connection);
    }

    public Map<Long, List<AffectedJourney>> queryAndProcessResults() throws SQLException {
        String dateNow = QueryUtils.localDateAsString(Instant.now(), timeZone);
        log.info("Querying affected journeys from database");
        try (PreparedStatement statement = dbConnection.prepareStatement(queryString)) {
            ResultSet resultSet = statement.executeQuery();
            return parseJourneys(resultSet);
        }
        catch (Exception e) {
            log.error("Error while  querying and processing affected journeys", e);
            throw e;
        }
    }

    private Map<Long, List<AffectedJourney>> parseJourneys(ResultSet resultSet) throws SQLException {
        Map<Long, List<AffectedJourney>> map = new HashMap<>();
        log.info("Processing affected journeys info resultset");
        while (resultSet.next()) {
            try {
                String tripId = resultSet.getString("DVJ_Id");
                String operatingDay = resultSet.getString("OPERATING_DAY");
                String routeName = resultSet.getString("ROUTE_NAME");
                int direction = resultSet.getInt("DIRECTION");
                String startTime = resultSet.getString("START_TIME");
                long journeyPatternId = resultSet.getLong("JP_Id");
                AffectedJourney affectedJourney = new AffectedJourney(tripId, operatingDay, routeName, direction, startTime, journeyPatternId);
                if (!map.containsKey(journeyPatternId)) {
                    map.put(journeyPatternId, new LinkedList<>());
                }
                map.get(journeyPatternId).add(affectedJourney);
            } catch (IllegalArgumentException iae) {
                log.error("Error while parsing affected journeys resultset", iae);
            }
        }
        for (Long journeyPatternId : map.keySet()) {
            log.info("Found {} journeys for affected journey pattern id {}", map.get(journeyPatternId).size(), journeyPatternId);
        }
        return map;
    }

}
