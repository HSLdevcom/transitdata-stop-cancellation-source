package fi.hsl.transitdata.stop.cancellations.disruption.route.source;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.stop.cancellations.db.QueryUtils;
import fi.hsl.transitdata.stop.cancellations.models.JourneyPattern;
import fi.hsl.transitdata.stop.cancellations.models.JourneyPatternStop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DoiAffectedJourneyPatternSource {

    private static final Logger log = LoggerFactory.getLogger(DoiAffectedJourneyPatternSource.class);
    private final Connection dbConnection;
    private final String queryString;
    private final String timeZone;

    private DoiAffectedJourneyPatternSource(PulsarApplicationContext context, Connection connection, boolean useTestDoiQueries) {
        dbConnection = connection;
        queryString = QueryUtils.createQuery(getClass(), useTestDoiQueries ? "/affected_journey_patterns_by_ids_test.sql" : "/affected_journey_patterns_by_ids.sql");
        timeZone = context.getConfig().getString("omm.timezone");
    }

    public static DoiAffectedJourneyPatternSource newInstance(PulsarApplicationContext context, String jdbcConnectionString, boolean useTestDoiQueries) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        return new DoiAffectedJourneyPatternSource(context, connection, useTestDoiQueries);
    }

    public Map<String, JourneyPattern> queryByJourneyPatternIds(List<String> journeyPatternIds) throws SQLException {
        log.info("Querying journey patterns by disruption routes from database");
        String dateNow = QueryUtils.localDateAsString(Instant.now(), timeZone);
        String queryJourneyPatternIds = String.join(",", journeyPatternIds);
        String preparedQueryString = queryString
                .replaceAll("VAR_DATE_NOW", dateNow)
                .replace("VAR_JP_IDS", queryJourneyPatternIds);

        try (PreparedStatement statement = dbConnection.prepareStatement(preparedQueryString)) {
            ResultSet resultSet = statement.executeQuery();
            return parseAffectedJourneyPatterns(resultSet);
        }
        catch (Exception e) {
            log.error("Error while  querying and processing messages", e);
            throw e;
        }
    }

    private Map<String, JourneyPattern> parseAffectedJourneyPatterns(ResultSet resultSet) throws SQLException {
        log.info("Processing journey pattern resultset by disruption routes");
        Map<String, JourneyPattern> journeyPatterns = new HashMap<>();
        while (resultSet.next()) {
            try {
                String stopGid = resultSet.getString("SP_Gid");
                String stopId = resultSet.getString("JPP_Number");
                String stopName = resultSet.getString("SP_Name");
                int stopSequence = resultSet.getInt("PIJP_SequenceNumber");
                String jpId = resultSet.getString("JP_Id");
                if (stopId != null) {
                    if (!journeyPatterns.containsKey(jpId)) {
                        int jpPointCount = resultSet.getInt("JP_PointCount");
                        journeyPatterns.put(jpId, new JourneyPattern(jpId, jpPointCount));
                    }
                    journeyPatterns.get(jpId).addStop(new JourneyPatternStop(stopGid, stopId, stopName, stopSequence));
                } else {
                    log.warn("found null stopId with sequence {} for jpId {}", stopSequence, jpId);
                }
            } catch (IllegalArgumentException iae) {
                log.error("Error while parsing the journey patterns for disruption routes", iae);
            }
        }
        log.info("found {} journey patterns for disruption routes", journeyPatterns.size());
        return journeyPatterns;
    }
}
