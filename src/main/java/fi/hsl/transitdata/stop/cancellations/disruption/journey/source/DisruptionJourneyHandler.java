package fi.hsl.transitdata.stop.cancellations.disruption.journey.source;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.stop.cancellations.db.DoiStopInfoSource;
import fi.hsl.transitdata.stop.cancellations.disruption.journey.source.models.DisruptionJourney;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public class DisruptionJourneyHandler {

    private static final Logger log = LoggerFactory.getLogger(DisruptionJourneyHandler.class);
    final DisruptionJourneysStopsSource disruptionJourneysStopsSource;

    public DisruptionJourneyHandler(PulsarApplicationContext context, String doiConnString) throws SQLException {
        disruptionJourneysStopsSource = DisruptionJourneysStopsSource.newInstance(context, doiConnString);

    }

    public Optional<InternalMessages.StopCancellations> queryAndProcessResults(DoiStopInfoSource doiStops) throws SQLException {
        List<DisruptionJourney> disruptionJourneys = disruptionJourneysStopsSource.queryAndProcessResults(doiStops.getStopsByGidMap());

        return Optional.empty();
    }

}
