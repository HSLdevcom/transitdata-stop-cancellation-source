package fi.hsl.transitdata.omm.models;

public class AffectedJourneyPatternStop {

    public long stopGid;
    public long stopId;
    public int sequence;
    public String name;

    public AffectedJourneyPatternStop(long stopGid, long stopId, String name, int sequence)  {
        this.stopGid = stopGid;
        this.stopId = stopId;
        this.name = name;
        this.sequence = sequence;
    }

}
