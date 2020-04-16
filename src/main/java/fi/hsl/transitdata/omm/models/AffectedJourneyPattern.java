package fi.hsl.transitdata.omm.models;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AffectedJourneyPattern {

    public long id;
    public int stopCount;
    public Map<Integer, AffectedJourneyPatternStop> stops;
    public List<Long> affectedDvjs;

    public AffectedJourneyPattern(long id, int stopCount) {
        this.id = id;
        this.stopCount = stopCount;
        this.stops = new HashMap<>();
        this.affectedDvjs = new ArrayList<>();
    }

    public void addStop(AffectedJourneyPatternStop stop) {
        if (!stops.containsKey(stop.sequence)) {
            stops.put(stop.sequence, stop);
        }
    }

    public void addAffectedDvj(long dvj) {
        if (!affectedDvjs.contains(dvj)) {
            affectedDvjs.add(dvj);
        }
    }
}
