package fi.hsl.transitdata.omm;

import fi.hsl.transitdata.omm.models.AffectedJourney;
import fi.hsl.transitdata.omm.models.AffectedJourneyPattern;
import fi.hsl.transitdata.omm.models.StopCancellation;

import java.util.List;
import java.util.Map;

public class StopCancellationUtils {

    public static void addAffectedJourneysToJourneyPatterns(
            Map<Long, AffectedJourneyPattern> affectedJourneyPatternMap,
            Map<Long, List<AffectedJourney>> affectedJourneysMap) {
        for (Map.Entry<Long, List<AffectedJourney>> entry : affectedJourneysMap.entrySet()) {
            affectedJourneyPatternMap.get(entry.getKey()).addAffectedJourneys(entry.getValue());
        }
    }

    public static void addAffectedJourneyPatternsToStopCancellations(
            List<StopCancellation> stopCancellations,
            Map<Long, AffectedJourneyPattern> affectedJourneyPatternMap) {
        for (StopCancellation stopCancellation : stopCancellations) {
            for (AffectedJourneyPattern journeyPattern : affectedJourneyPatternMap.values()) {
                if (journeyPattern.getStopIds().contains(stopCancellation.stopId)) {
                    stopCancellation.addAffectedJourneyPatternId(journeyPattern.id);
                }
            }
        }

    }
}
