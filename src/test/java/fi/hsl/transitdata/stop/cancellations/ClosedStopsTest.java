package fi.hsl.transitdata.stop.cancellations;

import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.stop.cancellations.closed.stop.source.ClosedStopHandler;
import fi.hsl.transitdata.stop.cancellations.closed.stop.source.models.ClosedStop;
import static org.junit.Assert.assertEquals;

import fi.hsl.transitdata.stop.cancellations.models.Journey;
import fi.hsl.transitdata.stop.cancellations.models.JourneyPattern;
import fi.hsl.transitdata.stop.cancellations.models.JourneyPatternStop;
import org.junit.Test;

import java.util.*;

public class ClosedStopsTest {

    @Test
    public void testClosedStopsMapping() {
        List<ClosedStop> closedStops = new ArrayList<>();
        closedStops.add(new ClosedStop("11", "4", "Valimo", 1, "peruttu", "2020-01-04", "2020-07-01", "Europe/Helsinki"));
        closedStops.add(new ClosedStop("12", "5", "Huopalahti", 2, "peruttu", "2020-01-04", "2020-07-01", "Europe/Helsinki"));
        closedStops.add(new ClosedStop("21", "6", "Rautatientori", 3, "peruttu", "2020-01-04", "2020-07-01", "Europe/Helsinki"));

        JourneyPattern journeyPattern1 = new JourneyPattern("50", 3);
        journeyPattern1.addStop(new JourneyPatternStop("1", "11", "Valimo", 1));
        journeyPattern1.addStop(new JourneyPatternStop("2", "12", "Huopalahti", 2));
        journeyPattern1.addStop(new JourneyPatternStop("3", "13", "Leppävaara", 3));

        JourneyPattern journeyPattern2 = new JourneyPattern("51", 4);
        journeyPattern2.addStop(new JourneyPatternStop("11", "21", "Rautatientori", 1));
        journeyPattern2.addStop(new JourneyPatternStop("12", "22", "Kurvi", 2));
        journeyPattern2.addStop(new JourneyPatternStop("13", "23", "Mäkelänrinne", 3));
        journeyPattern2.addStop(new JourneyPatternStop("14", "24", "Käpylän asema", 3));

        Map<String, JourneyPattern> affectedJourneyPatternMap = new HashMap<>();
        affectedJourneyPatternMap.put("50", journeyPattern1);
        affectedJourneyPatternMap.put("51", journeyPattern2);

        Map <String, List<Journey>> affectedJourneyMap = new HashMap<>();
        affectedJourneyMap.put("50", new ArrayList<>());
        affectedJourneyMap.get("50").add(new Journey("A1", "2020-04-21", "A", 1, "07:36:00", "50"));
        affectedJourneyMap.get("50").add(new Journey("A2", "2020-04-21", "A", 1, "07:50:00", "50"));
        affectedJourneyMap.put("51", new ArrayList<>());
        affectedJourneyMap.get("51").add(new Journey("671", "2020-04-21", "67", 1, "07:36:00", "51"));
        affectedJourneyMap.get("51").add(new Journey("672", "2020-04-21", "67", 1, "07:50:00", "51"));
        affectedJourneyMap.get("51").add(new Journey("673", "2020-04-21", "67", 1, "08:10:00", "51"));

        ClosedStopHandler.addAffectedJourneysToJourneyPatterns(affectedJourneyPatternMap, affectedJourneyMap);
        ClosedStopHandler.addAffectedJourneyPatternsToClosedStops(closedStops, affectedJourneyPatternMap);
        Optional<InternalMessages.StopCancellations> message = ClosedStopHandler.createStopCancellationsMessage(closedStops, new ArrayList<>(affectedJourneyPatternMap.values()));

        assertEquals(3, message.get().getStopCancellationsCount());
        assertEquals(1, message.get().getStopCancellations(0).getAffectedJourneyPatternIdsCount());
        assertEquals(1, message.get().getStopCancellations(1).getAffectedJourneyPatternIdsCount());
        assertEquals(1, message.get().getStopCancellations(2).getAffectedJourneyPatternIdsCount());

        assertEquals(2, message.get().getAffectedJourneyPatternsCount());
        assertEquals(2, message.get().getAffectedJourneyPatterns(0).getTripsCount());
        assertEquals(3, message.get().getAffectedJourneyPatterns(1).getTripsCount());

        assertEquals(3, message.get().getAffectedJourneyPatterns(0).getStopsCount());
        assertEquals(4, message.get().getAffectedJourneyPatterns(1).getStopsCount());
        assertEquals("11", message.get().getAffectedJourneyPatterns(0).getStops(0).getStopId());
        assertEquals("22", message.get().getAffectedJourneyPatterns(1).getStops(0).getStopId());
    }

}
