package fi.hsl.transitdata.omm;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.files.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import fi.hsl.transitdata.omm.models.StopCancellation;

import java.io.InputStream;
import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class OmmStopCancellationSource {

    private static final Logger log = LoggerFactory.getLogger(OmmStopCancellationSource.class);
    private final Connection dbConnection;
    private final String queryString;
    private final String timezone;

    private OmmStopCancellationSource(PulsarApplicationContext context, Connection connection) {
        dbConnection = connection;
        queryString = createQuery("/stop_cancellations.sql");
        timezone = context.getConfig().getString("omm.timezone");
    }

    public static OmmStopCancellationSource newInstance(PulsarApplicationContext context, String jdbcConnectionString) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        return new OmmStopCancellationSource(context, connection);
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

    static String localDateAsString(Instant instant, String zoneId) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd").format(instant.atZone(ZoneId.of(zoneId)));
    }

    public List<StopCancellation> queryAndProcessResults() throws SQLException {
        String dateNow = localDateAsString(Instant.now(), timezone);
        log.info("Querying stopCancellations from database");
        try (PreparedStatement statement = dbConnection.prepareStatement(queryString)) {
            statement.setString(1, dateNow);
            ResultSet resultSet = statement.executeQuery();
            return parseStopCancellations(resultSet);
        }
        catch (Exception e) {
            log.error("Error while  querying and processing messages", e);
            throw e;
        }
    }

    private List<StopCancellation> parseStopCancellations(ResultSet resultSet) throws SQLException {
        List<StopCancellation> stopCancellations = new ArrayList<>();
        log.info("Processing results");
        while (resultSet.next()) {
            try {
                long stopId = resultSet.getLong("SC_STOP_ID");
                long stopDeviationsid = resultSet.getLong("stop_deviations_id");
                String existsFromDate = resultSet.getString("SD_VALID_FROM");
                String existsUpToDate = resultSet.getString("SD_VALID_TO");
                String description = resultSet.getString("B_DESCRIPTION");
                stopCancellations.add(new StopCancellation(stopId, stopDeviationsid, description, existsFromDate, existsUpToDate, timezone));
            } catch (IllegalArgumentException iae) {
                log.error("Error while parsing the stopCancellation resultset", iae);
            }
        }
        return stopCancellations;
    }
}
