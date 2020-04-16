package fi.hsl.transitdata.omm.db;

import fi.hsl.common.files.FileUtils;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DoiAffectedTripsSource {

    private static final Logger log = LoggerFactory.getLogger(OmmStopCancellationSource.class);
    private final Connection dbConnection;
    private final String queryString;
    private final String timezone;

    private DoiAffectedTripsSource(PulsarApplicationContext context, Connection connection) {
        dbConnection = connection;
        queryString = createQuery("/affected_trips.sql");
        timezone = context.getConfig().getString("omm.timezone");
    }

    public static DoiAffectedTripsSource newInstance(PulsarApplicationContext context, String jdbcConnectionString) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        return new DoiAffectedTripsSource(context, connection);
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
