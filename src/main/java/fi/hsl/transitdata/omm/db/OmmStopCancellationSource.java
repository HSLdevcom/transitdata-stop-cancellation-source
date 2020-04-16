package fi.hsl.transitdata.omm.db;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.omm.models.Stop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import fi.hsl.transitdata.omm.models.StopCancellation;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OmmStopCancellationSource {

    private static final Logger log = LoggerFactory.getLogger(OmmStopCancellationSource.class);
    private final Connection dbConnection;
    private final String queryString;
    private final String timezone;

    private OmmStopCancellationSource(PulsarApplicationContext context, Connection connection) {
        dbConnection = connection;
        queryString = QueryUtils.createQuery(getClass() ,"/stop_cancellations.sql");
        timezone = context.getConfig().getString("omm.timezone");
    }

    public static OmmStopCancellationSource newInstance(PulsarApplicationContext context, String jdbcConnectionString) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        return new OmmStopCancellationSource(context, connection);
    }

    public List<StopCancellation> queryAndProcessResults(Map<Long, Stop> stopInfo) throws SQLException {
        String dateNow = QueryUtils.localDateAsString(Instant.now(), timezone);
        log.info("Querying stopCancellations from database");
        try (PreparedStatement statement = dbConnection.prepareStatement(queryString)) {
            statement.setString(1, dateNow);
            ResultSet resultSet = statement.executeQuery();
            return parseStopCancellations(resultSet, stopInfo);
        }
        catch (Exception e) {
            log.error("Error while  querying and processing messages", e);
            throw e;
        }
    }

    private List<StopCancellation> parseStopCancellations(ResultSet resultSet, Map<Long, Stop> stopInfo) throws SQLException {
        List<StopCancellation> stopCancellations = new ArrayList<>();
        log.info("Processing stopCancellations resultset");
        while (resultSet.next()) {
            try {
                long stopGid = resultSet.getLong("SC_STOP_ID");
                if (stopInfo.containsKey(stopGid)) {
                    Stop stop = stopInfo.get(stopGid);
                    long stopId = stop.stopId;
                    String stopName = stop.name;
                    long stopDeviationsid = resultSet.getLong("stop_deviations_id");
                    String existsFromDate = resultSet.getString("SD_VALID_FROM");
                    String existsUpToDate = resultSet.getString("SD_VALID_TO");
                    String description = resultSet.getString("B_DESCRIPTION");
                    stopCancellations.add(new StopCancellation(stopId, stopGid, stopName, stopDeviationsid, description, existsFromDate, existsUpToDate, timezone));
                    log.info("Reading cancelled stop {} ({}) with cancellation info: {}", stopName, stopId, description);
                } else {
                    log.error("Could not find stop info for cancelled stop (gid: {})", stopGid);
                }
            } catch (IllegalArgumentException iae) {
                log.error("Error while parsing the stopCancellation resultset", iae);
            }
        }
        log.info("Found {} stop cancellations", stopCancellations.size());
        return stopCancellations;
    }
}
