package fi.hsl.transitdata.omm.db;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.omm.models.AffectedJourney;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class DoiAffectedJourneySource {

    private static final Logger log = LoggerFactory.getLogger(DoiAffectedJourneySource.class);
    private final Connection dbConnection;
    private final String queryString;
    private final String timeZone;
    private final int queryFutureInDays;

    private DoiAffectedJourneySource(PulsarApplicationContext context, Connection connection) {
        dbConnection = connection;
        queryString = QueryUtils.createQuery(getClass(),"/affected_journeys.sql");
        timeZone = context.getConfig().getString("omm.timezone");
        queryFutureInDays = context.getConfig().getInt("doi.queryFutureJourneysInDays");
        log.info("Using {} future days in querying affected journeys", queryFutureInDays);
    }

    public static DoiAffectedJourneySource newInstance(PulsarApplicationContext context, String jdbcConnectionString) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        return new DoiAffectedJourneySource(context, connection);
    }

    public Map<String, List<AffectedJourney>> queryAndProcessResults(List<String> affectedJourneyPatterns) throws SQLException {
        log.info("Querying affected journeys from database");
        String dateFrom = QueryUtils.localDateAsString(Instant.now(), timeZone);
        String dateTo = QueryUtils.getOffsetDateAsString(Instant.now(), timeZone, queryFutureInDays);
        String affectedJourneyPatternIds = affectedJourneyPatterns.stream().collect(Collectors.joining(","));
        String preparedString = queryString
                .replace("VAR_FROM_DATE", dateFrom)
                .replace("VAR_TO_DATE", dateTo)
                .replace("VAR_AFFECTED_JP_IDS", affectedJourneyPatternIds);
        try (PreparedStatement statement = dbConnection.prepareStatement(preparedString)) {
            ResultSet resultSet = statement.executeQuery();
            return parseJourneys(resultSet);
        }
        catch (Exception e) {
            log.error("Error while  querying and processing affected journeys", e);
            throw e;
        }
    }

    private Map<String, List<AffectedJourney>> parseJourneys(ResultSet resultSet) throws SQLException {
        log.info("Processing affected journeys info resultset");
        Map<String, List<AffectedJourney>> map = new HashMap<>();
        while (resultSet.next()) {
            try {
                String tripId = resultSet.getString("DVJ_Id");
                String operatingDay = resultSet.getString("OPERATING_DAY");
                String routeName = resultSet.getString("ROUTE_NAME");
                int direction = resultSet.getInt("DIRECTION");
                String startTime = resultSet.getString("START_TIME");
                String journeyPatternId = resultSet.getString("JP_Id");
                AffectedJourney affectedJourney = new AffectedJourney(tripId, operatingDay, routeName, direction, startTime, journeyPatternId);
                if (!map.containsKey(journeyPatternId)) {
                    map.put(journeyPatternId, new LinkedList<>());
                }
                map.get(journeyPatternId).add(affectedJourney);
            } catch (IllegalArgumentException iae) {
                log.error("Error while parsing affected journeys resultset", iae);
            }
        }
        log.info("Found affected journeys for {} journey patterns", map.size());
        for (String journeyPatternId : map.keySet()) {
            log.info("Found {} journeys for affected journey pattern id {}", map.get(journeyPatternId).size(), journeyPatternId);
        }
        return map;
    }

}
