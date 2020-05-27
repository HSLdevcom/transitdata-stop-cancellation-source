package fi.hsl.transitdata.stop.cancellations;

import fi.hsl.transitdata.stop.cancellations.db.QueryUtils;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.*;

public class QueryUtilsTest {

    @Test
    public void testLocalDateAsString() {
        Instant now = Instant.parse("2020-04-16T23:26:59.877Z");
        assertEquals("2020-04-17", QueryUtils.localDateAsString(now, "Europe/Helsinki"));
    }

    @Test
    public void testGetOffsetDateAsString() {
        Instant now = Instant.parse("2020-04-16T23:26:59.877Z");
        assertEquals("2020-04-27", QueryUtils.getOffsetDateAsString(now, "Europe/Helsinki", 10));
    }

}
