package fi.hsl.transitdata.omm;

import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.omm.models.AffectedJourney;
import fi.hsl.transitdata.omm.models.AffectedJourneyPattern;
import fi.hsl.transitdata.omm.models.StopCancellation;
import org.apache.pulsar.client.api.PulsarClientException;

import javax.swing.text.html.Option;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class StopCancellationUtils {

    public static void addAffectedJourneysToJourneyPatterns(
            Map<String, AffectedJourneyPattern> affectedJourneyPatternMap,
            Map<String, List<AffectedJourney>> affectedJourneysMap) {
        for (Map.Entry<String, List<AffectedJourney>> entry : affectedJourneysMap.entrySet()) {
            affectedJourneyPatternMap.get(entry.getKey()).addAffectedJourneys(entry.getValue());
        }
    }

    public static void addAffectedJourneyPatternsToStopCancellations(
            List<StopCancellation> stopCancellations,
            Map<String, AffectedJourneyPattern> affectedJourneyPatternMap) {
        for (StopCancellation stopCancellation : stopCancellations) {
            for (AffectedJourneyPattern journeyPattern : affectedJourneyPatternMap.values()) {
                if (journeyPattern.getStopIds().contains(stopCancellation.stopId)) {
                    stopCancellation.addAffectedJourneyPatternId(journeyPattern.id);
                }
            }
        }

    }

    public static Optional<InternalMessages.StopCancellations> createStopCancellationsMessage(List<StopCancellation> stopCancellations, List<AffectedJourneyPattern> journeyPatterns)  {
        if (!stopCancellations.isEmpty()) {
            InternalMessages.StopCancellations.Builder builder = InternalMessages.StopCancellations.newBuilder();
            builder.addAllStopCancellations(stopCancellations.stream().map(sc -> sc.getAsProtoBuf()).collect(Collectors.toList()));
            builder.addAllAffectedJourneyPatterns(journeyPatterns.stream().map(jp -> jp.getAsProtoBuf()).collect(Collectors.toList()));
            return Optional.of(builder.build());
        } else {
            return Optional.empty();
        }
    }
}
