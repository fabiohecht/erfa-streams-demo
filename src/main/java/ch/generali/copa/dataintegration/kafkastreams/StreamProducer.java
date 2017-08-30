package ch.generali.copa.dataintegration.kafkastreams;

import ch.generali.copa.dataintegration.kafkastreams.integration.Partner;
import ch.generali.copa.dataintegration.kafkastreams.landing.Agent;
import ch.generali.copa.dataintegration.kafkastreams.landing.Data;
import ch.generali.copa.dataintegration.kafkastreams.landing.Headers;
import ch.generali.copa.dataintegration.kafkastreams.landing.operation;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.ws.rs.HEAD;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Created by fabio on 8/29/17.
 */
public class StreamProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        long events = 1L; //Long.parseLong(args[0]);

        Properties props = new Properties();
        // hardcoding the Kafka server URI for this example
        props.put("bootstrap.servers", "54.93.243.62:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://52.59.241.253:8081");
        // Hard coding topic too.
        String topic = "CORE_AGENTS";

        //TODO I assume here that Replicate will create change events using schema like Agents
        Producer<String, Agent> producer = new KafkaProducer<>(props);

        for (long i = 0; i < events; i++) {
            String id = "1";
            Data data = Data.newBuilder()
                    .setCOAGID(id)
                    .setCOAGACTIVE(1)
                    .setCOAGBIRTHDATE("25.01.1990")
                    .setCOAGCITY("Zürich")
                    .setCOAGCOUNTRY(1)
                    .setCOAGEMAIL("smith@example.com")
                    .setCOAGFAMILYNAME("Smith")
                    .setCOAGGENDER("M")
                    .setCOAGHOUSENR("123")
                    .setCOAGMOBILENR("079 555 44 75")
                    .setCOAGNAME("Johnny")
                    .setCOAGPHONENR("043 229 85 54")
                    .setCOAGPOSTALCODE("8001")
                    .setCOAGSTREET("Bahnhofstrasse")
                    .build();

            Headers headers = Headers.newBuilder()
                    .setOperation(operation.INSERT)
                    .setChangeSequence("1")
                    .setStreamPosition("5")
                    .setTimestamp(Long.toString(new Date().getTime()))
                    .setTransactionId(UUID.randomUUID().toString())
                    .build();

            Agent agent = Agent.newBuilder()
                    .setData(data)
                    .setBeforeData(null)
                    .setHeaders(headers)
                    .build();

            ProducerRecord<String, Agent> record = new ProducerRecord<>(topic, id, agent);
            producer.send(record).get();
        }
        producer.close();
    }
}
