package fi.hsl.transitdata.stop.cancellations.models;

import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JourneyPatternTest {
    @Test
    public void testStopsAreOrderedCorrectly() {
        JourneyPattern jp = new JourneyPattern("1", 4);
        jp.addStop(new JourneyPatternStop("1", "1", "stop_1", 1));
        jp.addStop(new JourneyPatternStop("2", "2", "stop_2", 2));
        jp.addStop(new JourneyPatternStop("3", "3", "stop_3", 3));
        jp.addStop(new JourneyPatternStop("4", "4", "stop_4", 4));

        Optional<List<JourneyPatternStop>> stops = jp.getStopsBetweenTwoStops("2", "4");
        assertTrue(stops.isPresent());
        assertEquals(1, stops.get().size());
        assertEquals("3", stops.get().get(0).stopId);
    }
}
