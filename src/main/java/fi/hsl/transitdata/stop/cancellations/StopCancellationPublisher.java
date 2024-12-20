package fi.hsl.transitdata.stop.cancellations;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class StopCancellationPublisher {

    private static final Logger log = LoggerFactory.getLogger(StopCancellationPublisher.class);
    private Producer<byte[]> producer;
    private String timeZone;

    public StopCancellationPublisher(PulsarApplicationContext context) {
        producer = context.getProducer();
        timeZone = context.getConfig().getString("omm.timezone");
    }

    public void sendStopCancellations(InternalMessages.StopCancellations message) throws PulsarClientException {
        final long currentTimestampUtcMs = ZonedDateTime.now(ZoneId.of(timeZone)).toInstant().toEpochMilli();
        sendStopCancellations(message, currentTimestampUtcMs);
    }

    private void sendStopCancellations(InternalMessages.StopCancellations message, long timestamp) throws PulsarClientException {
        List<String> stopIds = new ArrayList<>();
        if (message.getStopCancellationsList() != null) {
            stopIds = message.getStopCancellationsList().stream().map(x -> x.getStopId()).collect(Collectors.toList());
        }
        log.info("Sending {} stop cancellations with {} affected journey patterns. Stop ids: {}",
                message.getStopCancellationsCount(), message.getAffectedJourneyPatternsCount(), stopIds);
        try {
            producer.newMessage().value(message.toByteArray())
                    .eventTime(timestamp)
                    .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.StopCancellations.toString())
                    .send();
        }
        catch (PulsarClientException pe) {
            log.error("Failed to send stop cancellation message to Pulsar", pe);
            throw pe;
        }
        catch (Exception e) {
            log.error("Failed to handle handle stop cancellation message", e);
        }
    }

}
