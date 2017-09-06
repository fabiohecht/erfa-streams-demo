package ch.generali.copa.dataintegration.kafkastreams.producer;

import ch.generali.copa.dataintegration.kafkastreams.landing.Contract.CoreContract;
import ch.generali.copa.dataintegration.kafkastreams.landing.Contract.CoreContractRecord;
import ch.generali.copa.dataintegration.kafkastreams.landing.Contract.Headers;
import ch.generali.copa.dataintegration.kafkastreams.landing.Contract.operation;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.LocalDate;

import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Created by fabio on 9/1/17.
 */
public class ContractProducer {

    static void produceExampleContract(long events, String topic, Properties props) throws InterruptedException, ExecutionException {
        //TODO I assume here that Replicate will create change events using schema like Contracts
        Producer<String, CoreContract> producer = new KafkaProducer<>(props);

        for (int i = 0; i < events; i++) {
            CoreContractRecord data = CoreContractRecord.newBuilder()
                    //.setCOCOID(i+1)
                    .setCOCOID(i)
                    .setCOCOCOCUID(1)
                    .setCOCOCOAGID(i)
                    .setCOCOTYPE(3)
                    .setCOCOCOVERAGE(100000)
                    .setCOCOANNUALPREMIUM(100)
                    .setCOCOSTARTDATE(new LocalDate(2017, 9, 1))
                    .setCOCOENDDATE(new LocalDate(2020, 12, 31))
                    .setCOCOTOTALPAIDPREMIUMS(1000)
                    .setCOCOTOTALPAIDCLAIMS(0)
                    .build();

            String transactionId= UUID.randomUUID().toString();
            Headers headers = Headers.newBuilder()
                    .setOperation(operation.INSERT)
                    .setChangeSequence("1")
                    .setStreamPosition("5")
                    .setTimestamp(Long.toString(new Date().getTime()))
                    .setTransactionId(transactionId)
                    .build();

            CoreContract contract = CoreContract.newBuilder()
                    .setData(data)
                    .setBeforeData(null)
                    .setHeaders(headers)
                    .build();

            ProducerRecord<String, CoreContract> record = new ProducerRecord<>(topic, transactionId, contract);
            producer.send(record).get();

        }
        producer.close();
    }
}
