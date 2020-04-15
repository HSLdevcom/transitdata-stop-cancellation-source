package fi.hsl.transitdata.omm;

import java.io.File;
import java.sql.SQLException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.config.ConfigUtils;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.omm.models.StopCancellation;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        try {
            final Config config = ConfigParser.createConfig();

            final String ommConnString = readConnString("OMM_CONNECTION_STRING", "omm_conn_string");
            final String doiConnString = readConnString("DOI_CONNECTION_STRING", "doi_conn_string");

            final PulsarApplication app = PulsarApplication.newInstance(config);
            final PulsarApplicationContext context = app.getContext();
            final DoiStopInfoSource doiStops = DoiStopInfoSource.newInstance(context, doiConnString);
            final OmmStopCancellationSource omm = OmmStopCancellationSource.newInstance(context, ommConnString);
            final StopCancellationPublisher publisher = new StopCancellationPublisher(context);
            final int pollIntervalInSeconds = config.getInt("omm.interval");

            scheduler.scheduleAtFixedRate(() -> {
                try {
                    List<StopCancellation> stopCancellations = omm.queryAndProcessResults(doiStops.getStopInfo());
                    publisher.sendStopCancellations(stopCancellations);
                } catch (PulsarClientException e) {
                    log.error("Pulsar connection error", e);
                    closeApplication(app, scheduler);
                } catch (SQLException e) {
                    log.error("SQL exception", e);
                    closeApplication(app, scheduler);
                } catch (Exception e) {
                    log.error("Unknown exception at poll cycle: ", e);
                    closeApplication(app, scheduler);
                }
            }, 0, pollIntervalInSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Exception at Main: " + e.getMessage(), e);
        }
    }

    private static void closeApplication(PulsarApplication app, ScheduledExecutorService scheduler) {
        log.warn("Closing application");
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
        }
        if (app != null) {
            app.close();
        }
    }

    private static String readConnString(String envVar, String secretName) throws Exception {
        String connectionString = "";
        try {
            //Default path is what works with Docker out-of-the-box. Override with a local file if needed
            final String secretFilePath = ConfigUtils.getEnv(envVar)
                                                     .orElse("/run/secrets/" + secretName);
            connectionString = new Scanner(new File(secretFilePath))
                    .useDelimiter("\\Z").next();
        } catch (Exception e) {
            log.error("Failed to read DB connection string from secrets", e);
            throw e;
        }

        if (connectionString.isEmpty()) {
            throw new Exception("Failed to find DB connection string, exiting application");
        }
        return connectionString;
    }
}
