package fi.hsl.transitdata.omm.db;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.omm.models.AffectedJourneyPattern;
import fi.hsl.transitdata.omm.models.AffectedJourneyPatternStop;
import fi.hsl.transitdata.omm.models.StopCancellation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DoiAffectedJourneyPatternSource {

    private static final Logger log = LoggerFactory.getLogger(DoiAffectedJourneyPatternSource.class);
    private final Connection dbConnection;
    private final String queryString;
    private final String timeZone;

    private DoiAffectedJourneyPatternSource(PulsarApplicationContext context, Connection connection) {
        dbConnection = connection;
        queryString = QueryUtils.createQuery(getClass(), "/affected_journey_patterns.sql");
        timeZone = context.getConfig().getString("omm.timezone");
    }

    public static DoiAffectedJourneyPatternSource newInstance(PulsarApplicationContext context, String jdbcConnectionString) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        return new DoiAffectedJourneyPatternSource(context, connection);
    }

    public Map<Long, AffectedJourneyPattern> queryAndProcessResults(List<StopCancellation> stopCancellations) throws SQLException {
        log.info("Querying affected journey patterns from database");
        String dateNow = QueryUtils.localDateAsString(Instant.now(), timeZone);
        String affectedStops = stopCancellations.stream().map(sc -> String.valueOf(sc.stopGid)).collect(Collectors.joining(","));
        String preparedQueryString = queryString
                .replaceAll("VAR_DATE_NOW", "'"+dateNow+"'")
                .replace("VAR_AFFECTED_STOP_GIDS", affectedStops);

        try (PreparedStatement statement = dbConnection.prepareStatement(preparedQueryString)) {
            ResultSet resultSet = statement.executeQuery();
            return parseAffectedJourneyPatterns(resultSet);
        }
        catch (Exception e) {
            log.error("Error while  querying and processing messages", e);
            throw e;
        }
    }

    private Map<Long, AffectedJourneyPattern> parseAffectedJourneyPatterns(ResultSet resultSet) throws SQLException {
        Map<Long, AffectedJourneyPattern> affectedJourneyPatterns = new HashMap<>();
        log.info("Processing affected journey pattern resultset");
        while (resultSet.next()) {
            try {
                long stopGid = resultSet.getLong("SP_Gid");
                long stopId = resultSet.getLong("JPP_Number");
                String stopName = resultSet.getString("SP_Name");
                int stopSequence = resultSet.getInt("PIJP_SequenceNumber");
                AffectedJourneyPatternStop stop = new AffectedJourneyPatternStop(stopGid, stopId, stopName, stopSequence);
                long jpId = resultSet.getLong("JP_Id");
                if (!affectedJourneyPatterns.containsKey(jpId)) {
                    int jpPointCount = resultSet.getInt("JP_PointCount");
                    affectedJourneyPatterns.put(jpId, new AffectedJourneyPattern(jpId, jpPointCount));
                }
                affectedJourneyPatterns.get(jpId).addStop(stop);
            } catch (IllegalArgumentException iae) {
                log.error("Error while parsing the affected journey patterns resultset", iae);
            }
        }
        log.info("found {} affected journey patterns", affectedJourneyPatterns.size());
        return affectedJourneyPatterns;
    }
}
