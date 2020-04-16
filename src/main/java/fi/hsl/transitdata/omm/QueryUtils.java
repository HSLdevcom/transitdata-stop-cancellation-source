package fi.hsl.transitdata.omm;

import fi.hsl.common.files.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class QueryUtils {
    private static final Logger log = LoggerFactory.getLogger(OmmStopCancellationSource.class);

    public static String createQuery(Class c, String resourceFileName) {
        try {
            InputStream stream = c.getResourceAsStream(resourceFileName);
            return FileUtils.readFileFromStreamOrThrow(stream);
        } catch (Exception e) {
            log.error("Error in reading sql from file:", e);
            return null;
        }
    }

    public static String localDateAsString(Instant instant, String zoneId) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd").format(instant.atZone(ZoneId.of(zoneId)));
    }

}
