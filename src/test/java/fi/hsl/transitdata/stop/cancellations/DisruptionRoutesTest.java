package fi.hsl.transitdata.stop.cancellations;

import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.stop.cancellations.disruption.route.source.models.DisruptionRoute;
import fi.hsl.transitdata.stop.cancellations.models.Journey;
import fi.hsl.transitdata.stop.cancellations.models.JourneyPattern;
import fi.hsl.transitdata.stop.cancellations.models.JourneyPatternStop;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class DisruptionRoutesTest {

    private LocalDateTime getLocalDateTimeFromUnix(long unix) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(unix), ZoneId.of("Europe/Helsinki"));
    }

    @Test
    public void testCreateStopCancellationsByDisruptionRoutes() {

        // create journey patterns -> map
        JourneyPattern jp1 = new JourneyPattern("50", 3);
        jp1.addStop(new JourneyPatternStop("1", "22", "", 2));
        jp1.addStop(new JourneyPatternStop("2", "21", "", 1));
        jp1.addStop(new JourneyPatternStop("3", "23", "", 3));
        jp1.addStop(new JourneyPatternStop("4", "26", "", 6));
        jp1.addStop(new JourneyPatternStop("4", "28", "", 8));
        jp1.addStop(new JourneyPatternStop("4", "27", "", 7));
        jp1.addStop(new JourneyPatternStop("5", "25", "", 5));
        jp1.addStop(new JourneyPatternStop("6", "24", "", 4));

        JourneyPattern jp2 = new JourneyPattern("51", 4);
        jp2.addStop(new JourneyPatternStop("7", "34", "", 4));
        jp2.addStop(new JourneyPatternStop("10", "31", "", 1));
        jp2.addStop(new JourneyPatternStop("9", "32", "", 2));
        jp2.addStop(new JourneyPatternStop("8", "33", "", 3));

        Map<String, JourneyPattern> jpById = new HashMap<>();
        jpById.put(jp1.id, jp1);
        jpById.put(jp2.id, jp2);
        jpById.values().forEach(JourneyPattern::orderStopsBySequence);

        // create journeys
        Journey j1 = new Journey("A1", "2020-06-02", "A", 1, "07:36:00", "50");
        Journey j2 = new Journey("A1", "2020-06-02", "A", 1, "07:36:00", "50");
        Journey j3 = new Journey("A1", "2020-06-02", "A", 1, "07:36:00", "51");
        Journey j4 = new Journey("A1", "2020-06-02", "A", 1, "07:36:00", "51");

        // create disruption routes -> list
        DisruptionRoute dr1 = new DisruptionRoute("1", "21", "24", "4", "2020-06-01 09:40:00", "2020-06-10 09:40:00", "Europe/Helsinki");
        DisruptionRoute dr2 = new DisruptionRoute("2", "26", "28", "4", "2020-06-01 09:40:00", "2020-06-10 09:40:00", "Europe/Helsinki");
        DisruptionRoute dr3 = new DisruptionRoute("3", "31", "32", "4", "2020-06-01 09:40:00", "2020-06-10 09:40:00", "Europe/Helsinki");

        dr1.addAffectedJourneys(asList(j1, j2));
        dr2.addAffectedJourneys(asList(j1, j2));
        dr3.addAffectedJourneys(asList(j3, j4));

        assertEquals(Collections.singletonList("50"), dr1.getAffectedJourneyPatternIds());
        assertEquals(Collections.singletonList("50"), dr2.getAffectedJourneyPatternIds());
        assertEquals(Collections.singletonList("51"), dr3.getAffectedJourneyPatternIds());

        List<DisruptionRoute> drs = asList(dr1, dr2, dr3);

        for (DisruptionRoute dr : drs) {
            dr.findAddAffectedStops(jpById);
        }

        List<InternalMessages.StopCancellations.StopCancellation> scListDr1 = drs.get(0).getAsStopCancellations();
        List<InternalMessages.StopCancellations.StopCancellation> scListDr2 = drs.get(1).getAsStopCancellations();
        List<InternalMessages.StopCancellations.StopCancellation> scListDr3 = drs.get(2).getAsStopCancellations();

        // test number of cancelled stops
        assertEquals(2, scListDr1.size());
        assertEquals(1, scListDr2.size());
        assertEquals(0, scListDr3.size()); // no stop cancellations should be created if there are no stops between the start & end stops of disruption route

        // test that stop cancellation ids are the ones between start & end stops of the disruption routes
        assertEquals("22", scListDr1.get(0).getStopId());
        assertEquals("23", scListDr1.get(1).getStopId());
        assertEquals("27", scListDr2.get(0).getStopId());

        // test that time validity of the stop cancellations matches the time validity of the respective disruption routes
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        assertEquals(LocalDateTime.parse(dr1.getValidFrom().get(), formatter), getLocalDateTimeFromUnix(scListDr1.get(0).getValidFromUnixS()));
        assertEquals(LocalDateTime.parse(dr1.getValidTo().get(), formatter), getLocalDateTimeFromUnix(scListDr1.get(0).getValidToUnixS()));
        assertEquals(LocalDateTime.parse(dr2.getValidFrom().get(), formatter), getLocalDateTimeFromUnix(scListDr2.get(0).getValidFromUnixS()));
        assertEquals(LocalDateTime.parse(dr2.getValidTo().get(), formatter), getLocalDateTimeFromUnix(scListDr2.get(0).getValidToUnixS()));

        // test that other attributes are in line between stop cancellations and disruption routes
        assertEquals(dr1.getAffectedJourneyPatternIds(), scListDr1.get(0).getAffectedJourneyPatternIdsList());
        assertEquals(dr1.getAffectedJourneyPatternIds(), scListDr1.get(1).getAffectedJourneyPatternIdsList());
        assertEquals(dr2.getAffectedJourneyPatternIds(), scListDr2.get(0).getAffectedJourneyPatternIdsList());
        assertEquals(InternalMessages.StopCancellations.Cause.JOURNEY_PATTERN_DETOUR, scListDr1.get(0).getCause());
        assertEquals(InternalMessages.StopCancellations.Cause.JOURNEY_PATTERN_DETOUR, scListDr1.get(1).getCause());
        assertEquals(InternalMessages.StopCancellations.Cause.JOURNEY_PATTERN_DETOUR, scListDr2.get(0).getCause());
    }

}
