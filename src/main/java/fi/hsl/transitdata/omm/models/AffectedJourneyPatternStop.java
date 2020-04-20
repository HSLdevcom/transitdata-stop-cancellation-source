package fi.hsl.transitdata.omm.models;

import fi.hsl.common.transitdata.proto.InternalMessages;

public class AffectedJourneyPatternStop {

    private final long stopGid;
    public final long stopId;
    private final int sequence;
    private final String name;

    public AffectedJourneyPatternStop(long stopGid, long stopId, String name, int sequence)  {
        this.stopGid = stopGid;
        this.stopId = stopId;
        this.name = name;
        this.sequence = sequence;
    }

    public InternalMessages.JourneyPattern.Stop getAsProtoBuf() {
        InternalMessages.JourneyPattern.Stop.Builder builder = InternalMessages.JourneyPattern.Stop.newBuilder();
        builder.setStopId(String.valueOf(stopId));
        builder.setStopSequence(sequence);
        return builder.build();
    }

}
