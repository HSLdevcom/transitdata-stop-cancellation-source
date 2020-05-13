package fi.hsl.transitdata.stop.cancellations.disruption.route.source.models;

import fi.hsl.transitdata.stop.cancellations.models.Journey;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class DisruptionRoute {

    public final String disruptionRouteId;
    public final String startStopId;
    public final String endStopId;
    private final Optional<LocalDateTime> validFromDate;
    private final Optional<LocalDateTime> validToDate;
    DateTimeFormatter formatter;
    public final String affectedRoutes;
    private final Map<String, List<Journey>> affectedJourneysByJourneyPatternId;

    public DisruptionRoute(String disruptionRouteId, String startStopId, String endStopId, String affectedRoutes, String validFromDate, String validToDate) {
        this.disruptionRouteId = disruptionRouteId;
        this.startStopId = startStopId;
        this.endStopId = endStopId;
        this.validFromDate = getDateOrEmpty(validFromDate);
        this.validToDate = getDateOrEmpty(validToDate);
        this.affectedRoutes = affectedRoutes;
        this.affectedJourneysByJourneyPatternId = new HashMap<>();
        this.formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    }

    public void addAffectedJourneys(List<Journey> journeys) {
        for (Journey journey : journeys) {
            if (!affectedJourneysByJourneyPatternId.containsKey(journey.journeyPatternId)) {
                affectedJourneysByJourneyPatternId.put(journey.journeyPatternId, new ArrayList<>());
            }
            affectedJourneysByJourneyPatternId.get(journey.journeyPatternId).add(journey);
        }
    }

    public Optional<String> getValidFrom() {
        return this.validFromDate.map(localDateTime -> localDateTime.format(formatter));
    }

    public Optional<String> getValidTo() {
        return this.validToDate.map(localDateTime -> localDateTime.format(formatter));
    }

    public List<String> getAffectedJourneyPatternIds() {
        return new ArrayList<>(this.affectedJourneysByJourneyPatternId.keySet());
    }

    public Optional<LocalDateTime> getDateOrEmpty(String dateStr) {
        try {
            return Optional.of(LocalDateTime.parse(dateStr.replace(" ", "T")));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

}
