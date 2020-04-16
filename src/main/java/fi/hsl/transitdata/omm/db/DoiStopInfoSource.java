package fi.hsl.transitdata.omm.db;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.omm.models.Stop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class DoiStopInfoSource {

    private static final Logger log = LoggerFactory.getLogger(OmmStopCancellationSource.class);
    private final Connection dbConnection;
    private final String queryString;
    private final String timeZone;
    private final Map<Long, Stop> stopMap;

    private DoiStopInfoSource(PulsarApplicationContext context, Connection connection) throws SQLException {
        this.dbConnection = connection;
        this.queryString = QueryUtils.createQuery(getClass() ,"/stop_info.sql");
        this.timeZone = context.getConfig().getString("omm.timezone");;
        this.stopMap = queryAndProcessResults();
    }

    public static DoiStopInfoSource newInstance(PulsarApplicationContext context, String jdbcConnectionString) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        return new DoiStopInfoSource(context, connection);
    }

    public Map<Long, Stop> getStopInfo() {
        return stopMap;
    }

    public Map<Long, Stop> queryAndProcessResults() throws SQLException {
        String dateNow = QueryUtils.localDateAsString(Instant.now(), timeZone);
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
        log.info("Processing stop info resultset");
        while (resultSet.next()) {
            try {
                long stopGid = resultSet.getLong("SP_Gid");
                String name = resultSet.getString("SP_Name");
                long stopId = resultSet.getLong("JPP_Number");
                if (!map.containsKey(stopGid)) {
                    map.put(stopGid, new Stop(stopGid, stopId, name));
                }
            } catch (IllegalArgumentException iae) {
                log.error("Error while parsing the stop resultset", iae);
            }
        }
        log.info("Found total {} stops", map.size());
        return map;
    }

}
