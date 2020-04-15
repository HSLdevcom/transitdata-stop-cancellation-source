package fi.hsl.transitdata.omm;

import fi.hsl.common.files.FileUtils;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.omm.models.Stop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class DoiStopInfoSource {

    private static final Logger log = LoggerFactory.getLogger(OmmStopCancellationSource.class);
    private final Connection dbConnection;
    private final String queryString;
    private final String timeZone;
    private final Map<Long, Stop> stopMap;

    private DoiStopInfoSource(PulsarApplicationContext context, Connection connection) throws SQLException {
        dbConnection = connection;
        queryString = createQuery("/stop_info.sql");
        timeZone = context.getConfig().getString("omm.timezone");
        stopMap = queryAndProcessResults();
    }

    public static DoiStopInfoSource newInstance(PulsarApplicationContext context, String jdbcConnectionString) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        return new DoiStopInfoSource(context, connection);
    }

    public Map<Long, Stop> getStopInfo() {
        return stopMap;
    }

    static String localDateAsString(Instant instant, String zoneId) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd").format(instant.atZone(ZoneId.of(zoneId)));
    }

    public Map<Long, Stop> queryAndProcessResults() throws SQLException {
        String dateNow = localDateAsString(Instant.now(), timeZone);
        log.info("Querying stop info from database");
        try (PreparedStatement statement = dbConnection.prepareStatement(queryString)) {
            statement.setString(1, dateNow);
            statement.setString(2, dateNow);
            ResultSet resultSet = statement.executeQuery();
            return parseStops(resultSet);
        }
        catch (Exception e) {
            log.error("Error while  querying and processing stop info", e);
            throw e;
        }
    }

    private Map<Long, Stop> parseStops(ResultSet resultSet) throws SQLException {
        Map<Long, Stop> map = new HashMap<>();
        log.info("Processing results");
        while (resultSet.next()) {
            try {
                long stopGid = resultSet.getLong("STOP_POINT_GID");
                String name = resultSet.getString("STOP_POINT_NAME");
                long stopId = resultSet.getLong("JOURNEY_PATTERN_POINT_NUMBER");
                if (!map.containsKey(stopGid)) {
                    map.put(stopGid, new Stop(stopGid, stopId, name));
                }
            } catch (IllegalArgumentException iae) {
                log.error("Error while parsing the stop resultset", iae);
            }
        }
        return map;
    }

    private String createQuery(String resourceFileName) {
        try {
            InputStream stream = getClass().getResourceAsStream(resourceFileName);
            return FileUtils.readFileFromStreamOrThrow(stream);
        } catch (Exception e) {
            log.error("Error in reading sql from file:", e);
            return null;
        }
    }
}
