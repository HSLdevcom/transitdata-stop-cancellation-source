package fi.hsl.transitdata.omm.models;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AffectedJourneyPattern {

    public final long id;
    private final int stopCount;
    private final Map<Long, AffectedJourneyPatternStop> stops;
    private final List<AffectedJourney> affectedJourneys;

    public AffectedJourneyPattern(long id, int stopCount) {
        this.id = id;
        this.stopCount = stopCount;
        this.stops = new HashMap<>();
        this.affectedJourneys = new ArrayList<>();
    }

    public List<Long> getStopIds(){
        return new ArrayList<>(stops.keySet());
    }

    public void addStop(AffectedJourneyPatternStop stop) {
        if (!stops.containsKey(stop.stopId)) {
            stops.put(stop.stopId, stop);
        }
    }

    public void addAffectedJourneys(List<AffectedJourney> affectedJourneys) {
        this.affectedJourneys.addAll(affectedJourneys);
    }
}
