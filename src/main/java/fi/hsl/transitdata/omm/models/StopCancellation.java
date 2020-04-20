package fi.hsl.transitdata.omm.models;
import fi.hsl.common.transitdata.proto.InternalMessages;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class StopCancellation {

    public final String stopId;
    public final String stopGid;
    private final String stopName;
    private final long stopDeviationsId;
    private final String description;
    private final Optional<LocalDateTime> existsFromDate;
    private final Optional<LocalDateTime> existsUpToDate;
    private final String timezone;
    private final List<String> affectedJourneyPatternIds;

    public StopCancellation (String stopId, String stopGid, String stopName, long stopDeviationsId, String description, String existsFromDate, String existsUpToDate, String timezone) {
        this.stopId = stopId;
        this.stopGid = stopGid;
        this.stopName = stopName;
        this.stopDeviationsId = stopDeviationsId;
        this.description = description;
        this.existsFromDate = getDateOrEmpty(existsFromDate);
        this.existsUpToDate = getDateOrEmpty(existsUpToDate);
        this.timezone = timezone;
        this.affectedJourneyPatternIds = new ArrayList<>();
    }

    public Optional<LocalDateTime> getDateOrEmpty(String dateStr) {
        try {
            return Optional.of(LocalDateTime.parse(dateStr.replace(" ", "T")));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public void addAffectedJourneyPatternId(String journeyPatternId) {
        if (!affectedJourneyPatternIds.contains(journeyPatternId)) {
            affectedJourneyPatternIds.add(journeyPatternId);
        }
    }

    private Long toUtcEpochMs(LocalDateTime dt) {
        ZoneId zone = ZoneId.of(timezone);
        return dt.atZone(zone).toInstant().toEpochMilli();
    }

    public InternalMessages.StopCancellations.StopCancellation getAsProtoBuf() {
        InternalMessages.StopCancellations.StopCancellation.Builder builder = InternalMessages.StopCancellations.StopCancellation.newBuilder();
        builder.setStopId(stopId);
        existsFromDate.ifPresent(localDateTime -> builder.setValidFromUtcMs(toUtcEpochMs(localDateTime)));
        existsUpToDate.ifPresent(localDateTime -> builder.setValidToUtcMs(toUtcEpochMs(localDateTime)));
        if (!affectedJourneyPatternIds.isEmpty()) {
            builder.addAllAffectedJourneyPatternIds(new ArrayList<>(affectedJourneyPatternIds));
        }
        return builder.build();
    }

}
