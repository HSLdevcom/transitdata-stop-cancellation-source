package fi.hsl.transitdata.omm;

import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.omm.models.*;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

import java.util.*;

public class StopCancellationsTest {

    @Test
    public void testStopCancellationMapping() {
        List<StopCancellation> stopCancellations = new ArrayList<>();
        stopCancellations.add(new StopCancellation("11", "4", "Valimo", 1, "peruttu", "2020-01-04", "2020-07-01", "Europe/Helsinki"));
        stopCancellations.add(new StopCancellation("12", "5", "Huopalahti", 2, "peruttu", "2020-01-04", "2020-07-01", "Europe/Helsinki"));
        stopCancellations.add(new StopCancellation("21", "6", "Rautatientori", 3, "peruttu", "2020-01-04", "2020-07-01", "Europe/Helsinki"));

        AffectedJourneyPattern journeyPattern1 = new AffectedJourneyPattern("50", 3);
        journeyPattern1.addStop(new AffectedJourneyPatternStop("1", "11", "Valimo", 1));
        journeyPattern1.addStop(new AffectedJourneyPatternStop("2", "12", "Huopalahti", 2));
        journeyPattern1.addStop(new AffectedJourneyPatternStop("3", "13", "Leppävaara", 3));

        AffectedJourneyPattern journeyPattern2 = new AffectedJourneyPattern("51", 4);
        journeyPattern2.addStop(new AffectedJourneyPatternStop("11", "21", "Rautatientori", 1));
        journeyPattern2.addStop(new AffectedJourneyPatternStop("12", "22", "Kurvi", 2));
        journeyPattern2.addStop(new AffectedJourneyPatternStop("13", "23", "Mäkelänrinne", 3));
        journeyPattern2.addStop(new AffectedJourneyPatternStop("14", "24", "Käpylän asema", 3));

        Map<String, AffectedJourneyPattern> affectedJourneyPatternMap = new HashMap<>();
        affectedJourneyPatternMap.put("50", journeyPattern1);
        affectedJourneyPatternMap.put("51", journeyPattern2);

        Map <String, List<AffectedJourney>> affectedJourneyMap = new HashMap<>();
        affectedJourneyMap.put("50", new ArrayList<>());
        affectedJourneyMap.get("50").add(new AffectedJourney("A1", "2020-04-21", "A", 1, "07:36:00", "50"));
        affectedJourneyMap.get("50").add(new AffectedJourney("A2", "2020-04-21", "A", 1, "07:50:00", "50"));
        affectedJourneyMap.put("51", new ArrayList<>());
        affectedJourneyMap.get("51").add(new AffectedJourney("671", "2020-04-21", "67", 1, "07:36:00", "51"));
        affectedJourneyMap.get("51").add(new AffectedJourney("672", "2020-04-21", "67", 1, "07:50:00", "51"));
        affectedJourneyMap.get("51").add(new AffectedJourney("673", "2020-04-21", "67", 1, "08:10:00", "51"));

        StopCancellationUtils.addAffectedJourneysToJourneyPatterns(affectedJourneyPatternMap, affectedJourneyMap);
        StopCancellationUtils.addAffectedJourneyPatternsToStopCancellations(stopCancellations, affectedJourneyPatternMap);
        Optional<InternalMessages.StopCancellations> message = StopCancellationUtils.createStopCancellationsMessage(stopCancellations, new ArrayList<>(affectedJourneyPatternMap.values()));

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
