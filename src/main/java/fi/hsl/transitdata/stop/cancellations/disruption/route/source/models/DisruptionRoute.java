package fi.hsl.transitdata.stop.cancellations.disruption.route.source.models;

import java.time.LocalDateTime;
import java.util.Optional;

public class DisruptionRoute {

    public final String disruptionRouteId;
    public final String startStopId;
    public final String endStopId;
    private final Optional<LocalDateTime> validFromDate;
    private final Optional<LocalDateTime> validToDate;
    private final String affectedRoutes;
    
    public DisruptionRoute(String disruptionRouteId, String startStopId, String endStopId, String affectedRoutes, String validFromDate, String validToDate) {
        this.disruptionRouteId = disruptionRouteId;
        this.startStopId = startStopId;
        this.endStopId = endStopId;
        this.validFromDate = getDateOrEmpty(validFromDate);
        this.validToDate = getDateOrEmpty(validToDate);
        this.affectedRoutes = affectedRoutes;
    }

    public Optional<LocalDateTime> getDateOrEmpty(String dateStr) {
        try {
            return Optional.of(LocalDateTime.parse(dateStr.replace(" ", "T")));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

}
