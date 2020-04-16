package fi.hsl.transitdata.omm.models;

public class AffectedJourney {

    public String tripId;
    public String operatingDay;
    public String routeName;
    public int direction;
    public String startTime;
    public long journeyPatternId;

    public AffectedJourney(String tripid, String operatingDay, String routeName, int direction, String startTime, long journeyPatternId) {
        this.tripId = tripid;
        this.operatingDay = operatingDay;
        this.routeName = routeName;
        this.direction = direction;
        this.startTime = startTime;
        this.journeyPatternId = journeyPatternId;
    }

}
