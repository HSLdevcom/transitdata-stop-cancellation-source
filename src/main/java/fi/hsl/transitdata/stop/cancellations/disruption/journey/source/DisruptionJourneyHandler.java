package fi.hsl.transitdata.stop.cancellations.disruption.journey.source;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.stop.cancellations.db.DoiStopInfoSource;
import fi.hsl.transitdata.stop.cancellations.disruption.journey.source.models.DisruptionJourney;
import fi.hsl.transitdata.stop.cancellations.DoiAffectedJourneyPatternSource;
import fi.hsl.transitdata.stop.cancellations.models.JourneyPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class DisruptionJourneyHandler {

    private static final Logger log = LoggerFactory.getLogger(DisruptionJourneyHandler.class);
    final DisruptionJourneysStopsSource disruptionJourneysStopsSource;
    final DoiAffectedJourneyPatternSource affectedJourneyPatternSource;

    public DisruptionJourneyHandler(PulsarApplicationContext context, String doiConnString) throws SQLException {
        disruptionJourneysStopsSource = DisruptionJourneysStopsSource.newInstance(context, doiConnString);
        affectedJourneyPatternSource = DoiAffectedJourneyPatternSource.newInstance(context, doiConnString);
    }

    public Optional<InternalMessages.StopCancellations> queryAndProcessResults(DoiStopInfoSource doiStops) throws SQLException {
        List<DisruptionJourney> disruptionJourneys = disruptionJourneysStopsSource.queryAndProcessResults(doiStops.getStopsByGidMap());
        if (disruptionJourneys.size() == 0) return Optional.empty();

        List<String> affectedJourneyPatternIds = disruptionJourneys.stream().map(DisruptionJourney::getJourneyPatternId).distinct().collect(Collectors.toList());
        Map<String, JourneyPattern> affectedJourneyPatternsById = affectedJourneyPatternSource.queryByJourneyPatternIds(affectedJourneyPatternIds);

        InternalMessages.StopCancellations message = getStopCancellationsProtoBuf(disruptionJourneys, affectedJourneyPatternsById);
        return Optional.of(message);
    }

    public InternalMessages.StopCancellations getStopCancellationsProtoBuf(List<DisruptionJourney> disruptionJourneys, Map<String, JourneyPattern> affectedJourneyPatternsById) {
        List<InternalMessages.StopCancellations.StopCancellation> stopCancellations = disruptionJourneys.stream()
                .map(DisruptionJourney::getAsStopCancellation)
                .collect(Collectors.toList());

        List<InternalMessages.JourneyPattern> affectedJourneyPatterns = affectedJourneyPatternsById.values().stream()
                .map(JourneyPattern::getAsProtoBuf)
                .collect(Collectors.toList());

        InternalMessages.StopCancellations.Builder builder = InternalMessages.StopCancellations.newBuilder();
        builder.addAllStopCancellations(stopCancellations);
        builder.addAllAffectedJourneyPatterns(affectedJourneyPatterns);
        return builder.build();
    }

}
