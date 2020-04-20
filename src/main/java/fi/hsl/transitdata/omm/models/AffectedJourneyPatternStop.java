package fi.hsl.transitdata.omm.models;

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

}
