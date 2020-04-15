package fi.hsl.transitdata.omm.models;
import fi.hsl.common.transitdata.proto.InternalMessages;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

public class StopCancellation {

    public long stopId;
    public String stopName;
    public long stopDeviationsId;
    public String description;
    public Optional<LocalDateTime> existsFromDate;
    public Optional<LocalDateTime> existsUpToDate;
    private String timezone;

    public StopCancellation (long stopId, String stopName, long stopDeviationsId, String description, String existsFromDate, String existsUpToDate, String timezone) {
        this.stopId = stopId;
        this.stopName = stopName;
        this.stopDeviationsId = stopDeviationsId;
        this.description = description;
        this.existsFromDate = getDateOrEmpty(existsFromDate);
        this.existsUpToDate = getDateOrEmpty(existsUpToDate);
        this.timezone = timezone;
    }

    public Optional<LocalDateTime> getDateOrEmpty(String dateStr) {
        try {
            return Optional.of(LocalDateTime.parse(dateStr.replace(" ", "T")));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private Long toUtcEpochMs(LocalDateTime dt) {
        ZoneId zone = ZoneId.of(timezone);
        return dt.atZone(zone).toInstant().toEpochMilli();
    }

    public InternalMessages.StopCancellation getAsProtoBuf() {
        InternalMessages.StopCancellation.Builder builder = InternalMessages.StopCancellation.newBuilder();
        builder.setStopId(String.valueOf(stopId));
        if (existsFromDate.isPresent()) {
            builder.setValidFromUtcMs(toUtcEpochMs(existsFromDate.get()));
        }
        if (existsUpToDate.isPresent()) {
            builder.setValidToUtcMs(toUtcEpochMs(existsUpToDate.get()));
        }
        return builder.build();
    }

}
