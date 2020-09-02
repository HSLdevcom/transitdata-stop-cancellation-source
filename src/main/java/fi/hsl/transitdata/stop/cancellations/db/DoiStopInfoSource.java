package fi.hsl.transitdata.stop.cancellations.db;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.stop.cancellations.models.Stop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DoiStopInfoSource {
    private static final Logger log = LoggerFactory.getLogger(DoiStopInfoSource.class);

    private final Connection dbConnection;
    private final String queryString;
    private final String timeZone;

    //Cache stops for one day
    private Map<String, Stop> stopsMap;
    private Instant stopsUpdateTime;

    private DoiStopInfoSource(PulsarApplicationContext context, Connection connection, boolean useTestDoiQueries) throws SQLException {
        this.dbConnection = connection;
        this.queryString = QueryUtils.createQuery(getClass(), useTestDoiQueries ? "/stop_info_test.sql" : "/stop_info.sql");
        this.timeZone = context.getConfig().getString("omm.timezone");
    }

    public static DoiStopInfoSource newInstance(PulsarApplicationContext context, String jdbcConnectionString, boolean useTestDoiQueries) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        return new DoiStopInfoSource(context, connection, useTestDoiQueries);
    }

    /**
     * Returns a map with DOI stop info. Results are cached for one day
     * @return Map of DOI stops. Keys are stop GIDs
     * @throws SQLException
     */
    public Map<String, Stop> getDoiStopInfo() throws SQLException {
        final Instant currentTime = Instant.now();
        if (stopsUpdateTime == null || stopsUpdateTime.plus(1, ChronoUnit.DAYS).isBefore(currentTime)) {
            log.info("Querying stop info from database");
            String dateNow = QueryUtils.localDateAsString(currentTime, timeZone);

            try (PreparedStatement statement = dbConnection.prepareStatement(queryString.replaceAll("VAR_DATE_NOW", dateNow))) {
                ResultSet resultSet = statement.executeQuery();

                stopsMap = parseStops(resultSet);
                stopsUpdateTime = currentTime;

                log.info("Updated list of stops");
            } catch (SQLException sqle) {
                log.error("Error while querying and processing stop info", sqle);
                throw sqle;
            }
        }

        return Collections.unmodifiableMap(stopsMap);
    }

    private Map<String, Stop> parseStops(ResultSet resultSet) throws SQLException {
        log.info("Processing stop info resultset");
        Map<String, Stop> map = new HashMap<>();
        while (resultSet.next()) {
            try {
                String stopGid = resultSet.getString("SP_Gid");
                String name = resultSet.getString("SP_Name");
                String stopId = resultSet.getString("JPP_Number");
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
