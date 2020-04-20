package fi.hsl.transitdata.omm.models;

public class AffectedJourney {

    private final String tripId;
    private final String operatingDay;
    private final String routeName;
    private final int direction;
    private final String startTime;
    private final long journeyPatternId;

    public AffectedJourney(String tripid, String operatingDay, String routeName, int direction, String startTime, long journeyPatternId) {
        this.tripId = tripid;
        this.operatingDay = operatingDay;
        this.routeName = routeName;
        this.direction = direction;
        this.startTime = startTime;
        this.journeyPatternId = journeyPatternId;
    }

}
