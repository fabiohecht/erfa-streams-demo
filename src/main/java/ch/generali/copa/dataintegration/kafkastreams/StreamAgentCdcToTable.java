package ch.generali.copa.dataintegration.kafkastreams;

import ch.generali.copa.dataintegration.kafkastreams.landing.Agent.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;
import java.util.UUID;

/**
 * Created by fabio on 8/24/17.
 */
public class StreamAgentCdcToTable extends KafkaStreamWorker {

    private static final String INPUT_TOPIC = "CORE_AGENTS";
    private static final String OUTPUT_TOPIC_AGENTS = "CORE_AGENTS_RECORDS";

    static public void main(String[] args) {

        Properties config = loadProperties("kafka-streams.properties");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamAgentCdcToTable.class.toString());
        config.put(StreamsConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KStreamBuilder builder = new KStreamBuilder();
        final KStream<String, CoreAgentRecord> agentStream = builder.stream(INPUT_TOPIC)
                //TODO check what happens for a delete, do we get a null data and this would work deleting the record from a KTable referencing this topic?
                .filterNot((k, v) -> ((CoreAgent)v).getHeaders().getOperation().equals(operation.DELETE))
                .map((k, v) -> new KeyValue<>(
                        ((CoreAgent)v).getData().getCOAGID(), createAgentRecord(((CoreAgent)v).getData())));

        agentStream.to(OUTPUT_TOPIC_AGENTS);

        final KafkaStreams streams = new KafkaStreams(builder, config);

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static CoreAgentRecord createAgentRecord(CoreAgentRecord source) {
        //TODO check how we can reuse parts of the avro scheme
        return CoreAgentRecord.newBuilder()
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

}
