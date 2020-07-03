package fi.hsl.transitdata.stop.cancellations;

import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.stop.cancellations.db.DoiStopInfoSource;
import fi.hsl.transitdata.stop.cancellations.disruption.journey.source.DisruptionJourneyHandler;
import fi.hsl.transitdata.stop.cancellations.disruption.journey.source.DisruptionJourneysStopsSource;
import fi.hsl.transitdata.stop.cancellations.disruption.journey.source.models.DisruptionJourney;
import fi.hsl.transitdata.stop.cancellations.disruption.route.source.models.DisruptionRoute;
import fi.hsl.transitdata.stop.cancellations.models.Journey;
import fi.hsl.transitdata.stop.cancellations.models.JourneyPattern;
import fi.hsl.transitdata.stop.cancellations.models.JourneyPatternStop;
import org.apache.avro.generic.GenericData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DisruptionJourneysStopsSource.class, DoiAffectedJourneyPatternSource.class, LoggerFactory.class})
public class DisruptionJourneysTest {

    private LocalDateTime getLocalDateTimeFromUnix(long unix) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(unix), ZoneId.of("Europe/Helsinki"));
    }

    @Mock
    DisruptionJourneysStopsSource mockJourneyStopDataSource;

    @Mock
    DoiAffectedJourneyPatternSource mockAffectedJourneyPatternDataSource;

    @Mock
    Logger logger;

    @Mock
    DoiStopInfoSource mockDoiStopInfoSource;

    @Before
    public void setupMock() throws SQLException {
        PowerMockito.mockStatic(DisruptionJourneysStopsSource.class);
        PowerMockito.mockStatic(DoiAffectedJourneyPatternSource.class);
        PowerMockito.mockStatic(LoggerFactory.class);

        Mockito.when(LoggerFactory.getLogger(Mockito.any(Class.class))).thenReturn(logger);
        Mockito.when(DisruptionJourneysStopsSource.newInstance(null, null)).thenReturn(mockJourneyStopDataSource);
        Mockito.when(DoiAffectedJourneyPatternSource.newInstance(null, null)).thenReturn(mockAffectedJourneyPatternDataSource);
        Mockito.when(mockDoiStopInfoSource.getStopsByGidMap()).thenReturn(null);

        DisruptionJourney dj1 = new DisruptionJourney("7990504297881334","20180901", "1066K", 1, "24:48:00", "7990000038647169", "9022301111335001");
        DisruptionJourney dj2 = new DisruptionJourney("7990504297881334","20180901", "1066K", 1, "24:48:00", "7990000038647169", "9022301111337001");
        DisruptionJourney dj3 = new DisruptionJourney("7990504297881334","20180901", "1066K", 1, "24:48:00", "7990000038647169", "9022301111338001");
        DisruptionJourney dj4 = new DisruptionJourney("7990504297881334","20180901", "1066K", 1, "24:48:00", "7990000038647169", "9022301111357001");
        DisruptionJourney dj5 = new DisruptionJourney("7990504297881334","20180901", "1066K", 1, "24:48:00", "7990000038647169", "9022301111358001");
        List<DisruptionJourney> disruptedJourneys = Arrays.asList(dj1, dj2, dj3, dj4, dj5);
        when(mockJourneyStopDataSource.queryAndProcessResults(null)).thenReturn(disruptedJourneys);

        JourneyPattern journeyPattern = new JourneyPattern("7990000038647169", 1);
        Map<String, JourneyPattern> mapJourneyPattern = new HashMap<>();
        mapJourneyPattern.put(journeyPattern.id, journeyPattern);
        when(mockAffectedJourneyPatternDataSource.queryByJourneyPatternIds(Arrays.asList("7990000038647169"))).thenReturn(mapJourneyPattern);
    }

    @Test
    public void testCreateStopCancellationsByDisruptionRoutes() throws SQLException {

        DisruptionJourneyHandler journeyHandler = new DisruptionJourneyHandler(null, null);

        Optional<InternalMessages.StopCancellations> stopCancellations = journeyHandler.queryAndProcessResults(mockDoiStopInfoSource);
        assertEquals(stopCancellations.get().getStopCancellationsCount(), 5);
        assertEquals(stopCancellations.get().getAffectedJourneyPatternsCount(), 1);
    }

}
