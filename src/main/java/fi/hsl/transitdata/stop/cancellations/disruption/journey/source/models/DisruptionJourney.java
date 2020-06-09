package fi.hsl.transitdata.stop.cancellations.disruption.journey.source.models;

import fi.hsl.transitdata.stop.cancellations.models.Journey;

import java.util.ArrayList;
import java.util.List;

public class DisruptionJourney extends Journey {

    public final List<String> cancelledStops;

    public DisruptionJourney(String tripid, String operatingDay, String routeName, int direction, String startTime, String journeyPatternId) {
        super(tripid, operatingDay, routeName, direction, startTime, journeyPatternId);
        cancelledStops = new ArrayList<>();
    }

}
