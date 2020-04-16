package fi.hsl.transitdata.omm;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.omm.models.StopCancellation;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.ZonedDateTime;
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

    public void sendStopCancellations(List<StopCancellation> stopCancellations) throws PulsarClientException {
        if (!stopCancellations.isEmpty()) {
            InternalMessages.StopCancellations.Builder builder = InternalMessages.StopCancellations.newBuilder();
            builder.addAllStopCancellations(stopCancellations.stream().map(sc -> sc.getAsProtoBuf()).collect(Collectors.toList()));
            final long currentTimestampUtcMs = ZonedDateTime.now(ZoneId.of(timeZone)).toInstant().toEpochMilli();
            sendStopCancellations(builder.build(), currentTimestampUtcMs);
        }
    }

    private void sendStopCancellations(InternalMessages.StopCancellations message, long timestamp) throws PulsarClientException {
        log.info("Sending {} stop cancellations", message.getStopCancellationsCount());
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
