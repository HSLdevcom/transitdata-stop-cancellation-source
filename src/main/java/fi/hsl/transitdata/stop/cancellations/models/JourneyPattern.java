package fi.hsl.transitdata.stop.cancellations.models;

import fi.hsl.common.transitdata.proto.InternalMessages;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JourneyPattern {

    public final String id;
    private final int stopCount;
    private final Map<String, JourneyPatternStop> stops;
    private final List<Journey> journeys;
    private boolean stopsInOrder;

    public JourneyPattern(String id, int stopCount) {
        this.id = id;
        this.stopCount = stopCount;
        this.stops = new LinkedHashMap<>();
        this.journeys = new ArrayList<>();
        this.stopsInOrder = false;
    }

    public List<String> getStopIds(){
        return new ArrayList<>(stops.keySet());
    }

    public void addStop(JourneyPatternStop stop) {
        if (!stops.containsKey(stop.stopId)) {
            stops.put(stop.stopId, stop);
            stopsInOrder = false;
        }
    }

    public JourneyPattern createNewWithSameStops() {
        JourneyPattern copy = new JourneyPattern(id, stopCount);
        copy.stops.putAll(stops);
        copy.stopsInOrder = stopsInOrder;
        return copy;
    }

    public void addAffectedJourneys(List<Journey> affectedJourneys) {
        this.journeys.addAll(affectedJourneys);
    }

    public void orderStopsBySequence() {
        List<JourneyPatternStop> sortedStops = stops.values().stream()
                .sorted(Comparator.comparing(JourneyPatternStop::getSequence))
                .collect(Collectors.toList());
        stops.clear();
        for (JourneyPatternStop stop : sortedStops) {
            stops.put(stop.stopId, stop);
        }
        stopsInOrder = true;
    }

    public Optional<List<JourneyPatternStop>> getStopsBetweenTwoStops(String startStopId, String endStopId) {
        if (!stopsInOrder) orderStopsBySequence();
        ArrayList <JourneyPatternStop> stopsList = new ArrayList<>(stops.values());
        int startStopPosition = new ArrayList<>(stops.keySet()).indexOf(startStopId);
        int endStopPosition = new ArrayList<>(stops.keySet()).indexOf(endStopId);
        if (startStopPosition == -1 | endStopPosition == -1 | endStopPosition-startStopPosition <= 1) {
            return Optional.empty();
        }
        int[] stopIndexes = IntStream.range(startStopPosition + 1, endStopPosition).toArray();
        List<JourneyPatternStop> stopsBetween = Arrays.stream(stopIndexes).mapToObj(stopsList::get).collect(Collectors.toList());
        return stopsBetween.size() > 0 ? Optional.of(stopsBetween) : Optional.empty();
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
