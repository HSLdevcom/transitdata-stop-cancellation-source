package fi.hsl.transitdata.stop.cancellations.closed.stop.source;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.stop.cancellations.closed.stop.source.models.ClosedStop;
import fi.hsl.transitdata.stop.cancellations.db.DoiStopInfoSource;
import fi.hsl.transitdata.stop.cancellations.models.Journey;
import fi.hsl.transitdata.stop.cancellations.models.JourneyPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class ClosedStopHandler {

    private static final Logger log = LoggerFactory.getLogger(ClosedStopHandler.class);
    final OmmClosedStopSource closedStopSource;
    final DoiAffectedJourneyPatternSource affectedJourneyPatternSource;
    final DoiAffectedJourneySource affectedJourneySource;

    public ClosedStopHandler(PulsarApplicationContext context, String ommConnString, String doiConnString, String ommDatabaseName, String doiDatabaseName) throws SQLException {
        closedStopSource = OmmClosedStopSource.newInstance(context, ommConnString, ommDatabaseName);
        affectedJourneyPatternSource = DoiAffectedJourneyPatternSource.newInstance(context, doiConnString, doiDatabaseName);
        affectedJourneySource = DoiAffectedJourneySource.newInstance(context, doiConnString, doiDatabaseName);
    }

    public Optional<InternalMessages.StopCancellations> queryAndProcessResults(DoiStopInfoSource doiStops) throws SQLException{
        List<ClosedStop> closedStops = closedStopSource.queryAndProcessResults(doiStops.getStopsByGidMap());
        Map<String, JourneyPattern> affectedJourneyPatternById = affectedJourneyPatternSource.queryByClosedStops(closedStops);
        Map<String, List<Journey>> affectedJourneysByJourneyPatternId = affectedJourneySource.queryByJourneyPatternIds(affectedJourneyPatternById.keySet());
        addAffectedJourneysToJourneyPatterns(affectedJourneyPatternById, affectedJourneysByJourneyPatternId);
        addAffectedJourneyPatternsToClosedStops(closedStops, affectedJourneyPatternById);
        return createStopCancellationsMessage(closedStops, affectedJourneyPatternById.values());
    }

    public static void addAffectedJourneysToJourneyPatterns(
            Map<String, JourneyPattern> affectedJourneyPatternMap,
            Map<String, List<Journey>> affectedJourneysMap) {
        for (Map.Entry<String, List<Journey>> entry : affectedJourneysMap.entrySet()) {
            affectedJourneyPatternMap.get(entry.getKey()).addAffectedJourneys(entry.getValue());
        }
    }

    public static void addAffectedJourneyPatternsToClosedStops(
            List<ClosedStop> closedStops,
            Map<String, JourneyPattern> affectedJourneyPatternMap) {
        for (ClosedStop closedStop : closedStops) {
            for (JourneyPattern journeyPattern : affectedJourneyPatternMap.values()) {
                if (journeyPattern.getStopIds().contains(closedStop.stopId)) {
                    closedStop.addAffectedJourneyPatternId(journeyPattern.id);
                }
            }
        }

    }

    public static Optional<InternalMessages.StopCancellations> createStopCancellationsMessage(Collection<ClosedStop> closedStops, Collection<JourneyPattern> journeyPatterns)  {
        if (!closedStops.isEmpty()) {
            InternalMessages.StopCancellations.Builder builder = InternalMessages.StopCancellations.newBuilder();
            builder.addAllStopCancellations(closedStops.stream().map(ClosedStop::getAsProtoBuf).collect(Collectors.toList()));
            builder.addAllAffectedJourneyPatterns(journeyPatterns.stream().map(JourneyPattern::getAsProtoBuf).collect(Collectors.toList()));
            return Optional.of(builder.build());
        } else {
            return Optional.empty();
        }
    }

}
