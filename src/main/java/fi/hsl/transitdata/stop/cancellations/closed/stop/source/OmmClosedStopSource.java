package fi.hsl.transitdata.stop.cancellations.closed.stop.source;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.stop.cancellations.db.QueryUtils;
import fi.hsl.transitdata.stop.cancellations.models.Stop;
import fi.hsl.transitdata.stop.cancellations.closed.stop.source.models.ClosedStop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OmmClosedStopSource {

    private static final Logger log = LoggerFactory.getLogger(OmmClosedStopSource.class);
    private final Connection dbConnection;
    private final String queryString;
    private final String timezone;

    private OmmClosedStopSource(PulsarApplicationContext context, Connection connection, boolean useTestOmmQueries) {
        dbConnection = connection;
        queryString = QueryUtils.createQuery(getClass() , useTestOmmQueries ? "/closed_stops_test.sql" : "/closed_stops.sql");
        timezone = context.getConfig().getString("omm.timezone");
    }

    public static OmmClosedStopSource newInstance(PulsarApplicationContext context, String jdbcConnectionString, boolean useTestOmmQueries) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        return new OmmClosedStopSource(context, connection, useTestOmmQueries);
    }

    public List<ClosedStop> queryAndProcessResults(Map<String, Stop> stopInfo) throws SQLException {
        String dateNow = QueryUtils.localDateAsString(Instant.now(), timezone);
        log.info("Querying closed stops from database");
        try (PreparedStatement statement = dbConnection.prepareStatement(queryString)) {
            statement.setString(1, dateNow);
            ResultSet resultSet = statement.executeQuery();
            return parseClosedStops(resultSet, stopInfo);
        }
        catch (Exception e) {
            log.error("Error while  querying and processing messages", e);
            throw e;
        }
    }

    private List<ClosedStop> parseClosedStops(ResultSet resultSet, Map<String, Stop> stopInfo) throws SQLException {
        List<ClosedStop> closedStops = new ArrayList<>();
        log.info("Processing closed stops resultset");
        while (resultSet.next()) {
            try {
                String stopGid = resultSet.getString("SC_STOP_ID");
                if (stopInfo.containsKey(stopGid)) {
                    Stop stop = stopInfo.get(stopGid);
                    String stopId = stop.stopId;
                    String stopName = stop.name;
                    long stopDeviationsId = resultSet.getLong("stop_deviations_id");
                    String existsFromDate = resultSet.getString("SD_VALID_FROM");
                    String existsUpToDate = resultSet.getString("SD_VALID_TO");
                    String description = resultSet.getString("B_DESCRIPTION");
                    closedStops.add(new ClosedStop(stopId, stopGid, stopName, stopDeviationsId, description, existsFromDate, existsUpToDate, timezone));
                    log.info("Found closed stop {} ({}) with info: {} - stopDeviationsId: {}", stopName, stopId, description, stopDeviationsId);
                } else {
                    log.error("Could not find stop info for closed stop (gid: {})", stopGid);
                }
            } catch (IllegalArgumentException iae) {
                log.error("Error while parsing the closed stop resultset", iae);
            }
        }
        log.info("Found {} closed stops", closedStops.size());
        return closedStops;
    }
}
