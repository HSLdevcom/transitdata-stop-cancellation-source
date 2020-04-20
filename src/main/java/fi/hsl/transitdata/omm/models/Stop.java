package fi.hsl.transitdata.omm.models;

public class Stop {

    public final long stopGid;
    public final long stopId;
    public final String name;

    public Stop(long stopGid, long stopId, String name)  {
        this.stopGid = stopGid;
        this.stopId = stopId;
        this.name = name;
    }

}
