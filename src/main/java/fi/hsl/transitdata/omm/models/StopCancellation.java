package fi.hsl.transitdata.omm.models;

import java.time.LocalDateTime;
import java.util.Optional;

public class StopCancellation {

    public long stopId;
    public long stopDeviationsId;
    public String description;
    public Optional<LocalDateTime> existsFromDate;
    public Optional<LocalDateTime> existsUpToDate;

    public StopCancellation (long stopId, long stopDeviationsId, String description, String existsFromDate, String existsUpToDate) {
        this.stopId = stopId;
        this.stopDeviationsId = stopDeviationsId;
        this.description = description;
        this.existsFromDate = getDateOrEmpty(existsFromDate);
        this.existsUpToDate = getDateOrEmpty(existsUpToDate);
    }

    public Optional<LocalDateTime> getDateOrEmpty(String dateStr) {
        try {
            return Optional.of(LocalDateTime.parse(dateStr.replace(" ", "T")));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

}
