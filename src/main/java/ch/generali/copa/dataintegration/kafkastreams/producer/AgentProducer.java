package ch.generali.copa.dataintegration.kafkastreams.producer;

import ch.generali.copa.dataintegration.kafkastreams.landing.Agent.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Created by fabio on 9/1/17.
 */
public class AgentProducer {

    static void produceExampleAgent(long events, String topic, Properties props) throws InterruptedException, ExecutionException {
        //TODO I assume here that Replicate will create change events using schema like Agents
        Producer<String, CoreAgent> producer = new KafkaProducer<>(props);

        for (long i = 0; i < events; i++) {
            String id = "1";
            CoreAgentRecord data = CoreAgentRecord.newBuilder()
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

            String transactionId= UUID.randomUUID().toString();
            Headers headers = Headers.newBuilder()
                    .setOperation(ch.generali.copa.dataintegration.kafkastreams.landing.Agent.operation.INSERT)
                    .setChangeSequence("1")
                    .setStreamPosition("5")
                    .setTimestamp(Long.toString(new Date().getTime()))
                    .setTransactionId(transactionId)
                    .build();

            CoreAgent agent = CoreAgent.newBuilder()
                    .setData(data)
                    .setBeforeData(null)
                    .setHeaders(headers)
                    .build();

            ProducerRecord<String, CoreAgent> record = new ProducerRecord<>(topic, transactionId, agent);
            producer.send(record).get();

        }
        producer.close();
    }
}
