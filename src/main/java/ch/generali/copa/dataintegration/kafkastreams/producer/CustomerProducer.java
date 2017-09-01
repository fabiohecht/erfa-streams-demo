package ch.generali.copa.dataintegration.kafkastreams.producer;

import ch.generali.copa.dataintegration.kafkastreams.landing.Customer.Customer;
import ch.generali.copa.dataintegration.kafkastreams.landing.Customer.CoreCustomerRecord;
import ch.generali.copa.dataintegration.kafkastreams.landing.Customer.Headers;
import ch.generali.copa.dataintegration.kafkastreams.landing.Customer.operation;
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
public class CustomerProducer {

    static void produceExampleCustomer(long events, String topic, Properties props) throws InterruptedException, ExecutionException {
        //TODO I assume here that Replicate will create change events using schema like Customers
        Producer<String, Customer> producer = new KafkaProducer<>(props);

        for (long i = 0; i < events; i++) {
            String id = "1";
            CoreCustomerRecord data = CoreCustomerRecord.newBuilder()
                    .setCOCUID(id)
                    .setCOCUTYP(1)
                    .setCOCUFAMILYNAME("Doe")
                    .setCOCUMOBILENR("079 555 44 75")
                    .setCOCUBIRTHDATE(new LocalDate(1980, 12, 16))
                    .setCOCUCITY("Adliswil")
                    .setCOCUCOUNTRY("CH")
                    .setCOCUEMAILPRIVATE("doe@private.com")
                    .setCOCUEMAILWORK("doe@work.com")
                    .setCOCUGENDER("M")
                    .setCOCUHOUSENR("123")
                    .setCOCUNAME("John")
                    .setCOCUPHONENR("043 229 85 54")
                    .setCOCUPOSTALCODE("8001")
                    .setCOCUSTREET("Bahnhofstrasse")
                    .build();

            String transactionId= UUID.randomUUID().toString();
            Headers headers = Headers.newBuilder()
                    .setOperation(operation.INSERT)
                    .setChangeSequence("1")
                    .setStreamPosition("5")
                    .setTimestamp(Long.toString(new Date().getTime()))
                    .setTransactionId(transactionId)
                    .build();

            Customer customer = Customer.newBuilder()
                    .setData(data)
                    .setBeforeData(null)
                    .setHeaders(headers)
                    .build();

            ProducerRecord<String, Customer> record = new ProducerRecord<>(topic, transactionId, customer);
            producer.send(record).get();

        }
        producer.close();
    }
}
