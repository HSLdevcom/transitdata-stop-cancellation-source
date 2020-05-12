package fi.hsl.transitdata.stop.cancellations.models;

import fi.hsl.common.transitdata.proto.InternalMessages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JourneyPattern {

    public final String id;
    private final int stopCount;
    private final Map<String, JourneyPatternStop> stops;
    private final List<Journey> journeys;

    public JourneyPattern(String id, int stopCount) {
        this.id = id;
        this.stopCount = stopCount;
        this.stops = new HashMap<>();
        this.journeys = new ArrayList<>();
    }

    public List<String> getStopIds(){
        return new ArrayList<>(stops.keySet());
    }

    public void addStop(JourneyPatternStop stop) {
        if (!stops.containsKey(stop.stopId)) {
            stops.put(stop.stopId, stop);
        }
    }

    public void addAffectedJourneys(List<Journey> affectedJourneys) {
        this.journeys.addAll(affectedJourneys);
    }

    public InternalMessages.JourneyPattern getAsProtoBuf() {
        InternalMessages.JourneyPattern.Builder builder = InternalMessages.JourneyPattern.newBuilder();
        builder.setJourneyPatternId(id);
        builder.addAllStops(stops.values().stream().map(JourneyPatternStop::getAsProtoBuf).collect(Collectors.toList()));
        if (!journeys.isEmpty()) {
            builder.addAllTrips(journeys.stream().map(Journey::getAsProtoBuf).collect(Collectors.toList()));
        }
        return builder.build();
    }
}
