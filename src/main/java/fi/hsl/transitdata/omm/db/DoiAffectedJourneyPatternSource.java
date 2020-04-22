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
    private final int queryFutureInDays;

    private DoiAffectedJourneyPatternSource(PulsarApplicationContext context, Connection connection) {
        dbConnection = connection;
        queryString = QueryUtils.createQuery(getClass(), "/affected_journey_patterns.sql");
        timeZone = context.getConfig().getString("omm.timezone");
        queryFutureInDays = context.getConfig().getInt("doi.queryFutureJourneysInDays");
    }

    public static DoiAffectedJourneyPatternSource newInstance(PulsarApplicationContext context, String jdbcConnectionString) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        return new DoiAffectedJourneyPatternSource(context, connection);
    }

    public Map<String, AffectedJourneyPattern> queryAndProcessResults(List<StopCancellation> stopCancellations) throws SQLException {
        log.info("Querying affected journey patterns from database");
        String dateNow = QueryUtils.localDateAsString(Instant.now(), timeZone);
        String dateTo = QueryUtils.getOffsetDateAsString(Instant.now(), timeZone, queryFutureInDays);
        String affectedStops = stopCancellations.stream().map(sc -> sc.stopGid).collect(Collectors.joining(","));
        String preparedQueryString = queryString
                .replaceAll("VAR_DATE_NOW", dateNow)
                .replace("VAR_TO_DATE", dateTo)
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

    private Map<String, AffectedJourneyPattern> parseAffectedJourneyPatterns(ResultSet resultSet) throws SQLException {
        log.info("Processing affected journey pattern resultset");
        Map<String, AffectedJourneyPattern> affectedJourneyPatterns = new HashMap<>();
        while (resultSet.next()) {
            try {
                String stopGid = resultSet.getString("SP_Gid");
                String stopId = resultSet.getString("JPP_Number");
                String stopName = resultSet.getString("SP_Name");
                int stopSequence = resultSet.getInt("PIJP_SequenceNumber");
                String jpId = resultSet.getString("JP_Id");
                if (stopId != null) {
                    if (!affectedJourneyPatterns.containsKey(jpId)) {
                        int jpPointCount = resultSet.getInt("JP_PointCount");
                        affectedJourneyPatterns.put(jpId, new AffectedJourneyPattern(jpId, jpPointCount));
                    }
                    affectedJourneyPatterns.get(jpId).addStop(new AffectedJourneyPatternStop(stopGid, stopId, stopName, stopSequence));
                } else {
                    log.warn("found null stopId with sequence {} for jpId {}", stopSequence, jpId);
                }
            } catch (IllegalArgumentException iae) {
                log.error("Error while parsing the affected journey patterns resultset", iae);
            }
        }
        log.info("found {} affected journey patterns", affectedJourneyPatterns.size());
        return affectedJourneyPatterns;
    }
}
