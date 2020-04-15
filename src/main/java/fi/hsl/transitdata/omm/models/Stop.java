package fi.hsl.transitdata.omm.models;

public class Stop {

    public long stopGid;
    public long stopId;
    public String name;

    public Stop(long stopGid, long stopId, String name)  {
        this.stopGid = stopGid;
        this.stopId = stopId;
        this.name = name;
    }

}
