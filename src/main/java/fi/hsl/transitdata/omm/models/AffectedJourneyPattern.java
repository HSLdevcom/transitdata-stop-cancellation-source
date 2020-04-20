package fi.hsl.transitdata.omm.models;

import com.google.protobuf.Internal;
import fi.hsl.common.transitdata.proto.InternalMessages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AffectedJourneyPattern {

    public final String id;
    private final int stopCount;
    private final Map<String, AffectedJourneyPatternStop> stops;
    private final List<AffectedJourney> affectedJourneys;

    public AffectedJourneyPattern(String id, int stopCount) {
        this.id = id;
        this.stopCount = stopCount;
        this.stops = new HashMap<>();
        this.affectedJourneys = new ArrayList<>();
    }

    public List<String> getStopIds(){
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

    public InternalMessages.JourneyPattern getAsProtoBuf() {
        InternalMessages.JourneyPattern.Builder builder = InternalMessages.JourneyPattern.newBuilder();
        builder.setJourneyPatternId(id);
        builder.addAllStops(stops.values().stream().map(AffectedJourneyPatternStop::getAsProtoBuf).collect(Collectors.toList()));
        if (!affectedJourneys.isEmpty()) {
            builder.addAllTrips(affectedJourneys.stream().map(AffectedJourney::getAsProtoBuf).collect(Collectors.toList()));
        }
        return builder.build();
    }
}
