package fi.hsl.transitdata.stop.cancellations.db;

import fi.hsl.common.files.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class QueryUtils {
    private static final Logger log = LoggerFactory.getLogger(QueryUtils.class);

    public static String createQuery(Class c, String resourceFileName) {
        try (InputStream stream = c.getResourceAsStream(resourceFileName)) {
            return FileUtils.readFileFromStreamOrThrow(stream);
        } catch (Exception e) {
            log.error("Error in reading sql from file:", e);
            return null;
        }
    }

    public static String localDateAsString(Instant instant, String zoneId) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd").format(instant.atZone(ZoneId.of(zoneId)));
    }

    public static String getOffsetDateAsString(Instant instant, String zoneId, int offsetInDays) {
        ZonedDateTime then = instant.atZone(ZoneId.of(zoneId)).plus(offsetInDays, ChronoUnit.DAYS);
        String formattedString = DateTimeFormatter.ISO_LOCAL_DATE.format(then);
        log.debug("offsetInDays results to date " + formattedString);
        return formattedString;
    }
}
