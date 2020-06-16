package fi.hsl.transitdata.stop.cancellations.disruption.journey.source.models;

import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.stop.cancellations.models.Journey;

public class DisruptionJourney extends Journey {

    private String cancelledStopId;

    public DisruptionJourney(String tripid, String operatingDay, String routeName, int direction, String startTime, String journeyPatternId, String cancelledStopId) {
        super(tripid, operatingDay, routeName, direction, startTime, journeyPatternId);
        this.cancelledStopId = cancelledStopId;
    }

    public String getJourneyPatternId() {
        return this.journeyPatternId;
    }

    public InternalMessages.StopCancellations.StopCancellation  getAsStopCancellation() {
        InternalMessages.StopCancellations.StopCancellation.Builder builder = InternalMessages.StopCancellations.StopCancellation.newBuilder();
        builder.setStopId(cancelledStopId);
        builder.setCause(InternalMessages.StopCancellations.Cause.JOURNEY_DETOUR);
        builder.setAffectedTrip(this.getAsProtoBuf());
        builder.addAffectedJourneyPatternIds(this.journeyPatternId);
        return builder.build();
    }

}
