package ch.generali.copa.dataintegration.kafkastreams;

import ch.generali.copa.dataintegration.kafkastreams.landing.Agent;
import ch.generali.copa.dataintegration.kafkastreams.landing.AgentRecord;
import ch.generali.copa.dataintegration.kafkastreams.landing.Data;
import ch.generali.copa.dataintegration.kafkastreams.landing.operation;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Reducer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by fabio on 8/24/17.
 */
public class StreamAgentCdcToTable {

    private static final String INPUT_TOPIC = "CORE_AGENTS";
    private static final String OUTPUT_TOPIC_AGENTS = "CORE_AGENTS_RECORDS";

    static public void main(String[] args) {

        Properties config = loadProperties("kafka-streams.properties");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        //config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

//        System.out.println(config);
//        System.out.println(config.stringPropertyNames());

        final KStreamBuilder builder = new KStreamBuilder();
        final KStream<String, AgentRecord> agentStream = builder.stream(INPUT_TOPIC)
                .filterNot((k, v) -> ((Agent)v).getHeaders().getOperation().equals(operation.DELETE))
                .map((k, v) -> new KeyValue<>(
                        ((Agent)v).getData().getCOAGID(), createAgentRecord(((Agent)v).getData())));

        agentStream.to(OUTPUT_TOPIC_AGENTS);

        final KafkaStreams streams = new KafkaStreams(builder, config);
        // Always (and unconditionally) clean local state prior to starting the processing topology.
        // We opt for this unconditional call here because this will make it easier for you to play around with the example
        // when resetting the application for doing a re-run (via the Application Reset Tool,
        // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
        //
        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
        // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
        // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
        // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
        // See `ApplicationResetExample.java` for a production-like example.
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static AgentRecord createAgentRecord(Data source) {
        //TODO check how we can reuse parts of the avro scheme
        return AgentRecord.newBuilder()
                .setCOAGMOBILENR(source.getCOAGMOBILENR())
                .setCOAGACTIVE(source.getCOAGACTIVE())
                .setCOAGHOUSENR(source.getCOAGHOUSENR())
                .setCOAGCITY(source.getCOAGCITY())
                .setCOAGFAMILYNAME(source.getCOAGFAMILYNAME())
                .setCOAGPHONENR(source.getCOAGPHONENR())
                .setCOAGCOUNTRY(source.getCOAGCOUNTRY())
                .setCOAGEMAIL(source.getCOAGEMAIL())
                .setCOAGBIRTHDATE(source.getCOAGBIRTHDATE())
                .setCOAGNAME(source.getCOAGNAME())
                .setCOAGID(source.getCOAGID())
                .setCOAGSTREET(source.getCOAGSTREET())
                .setCOAGPOSTALCODE(source.getCOAGPOSTALCODE())
                .setCOAGGENDER(source.getCOAGGENDER())
                .build();
    }

    static private Properties loadProperties(String filename) {

        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = StreamAgentCdcToTable.class.getClassLoader().getResourceAsStream(filename);
            if (input == null) {
                throw new RuntimeException("Unable to find configuration file " + filename);
            }

            //load a properties file from class path, inside static method
            prop.load(input);

            return prop;

        } catch (IOException e) {
            throw new RuntimeException("Unable to read configuration file " + filename, e);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    throw new RuntimeException("Unable to read configuration file " + filename, e);
                }
            }
        }

    }

}
