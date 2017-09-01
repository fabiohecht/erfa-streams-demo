package ch.generali.copa.dataintegration.kafkastreams;

import ch.generali.copa.dataintegration.kafkastreams.integration.*;
import ch.generali.copa.dataintegration.kafkastreams.landing.Customer.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.*;

import static ch.generali.copa.dataintegration.kafkastreams.KafkaStreamWorker.loadProperties;

/**
 * Created by fabio on 8/31/17.
 */
public class StreamCoreCustomersToIntegration {

    private static final String INPUT_TOPIC_CUSTOMERS = "CORE_CUSTOMERS";
    private static final String OUTPUT_TOPIC_PARTNERS = "INTE_PARTNERS";

    static public void main(String[] args) {

        Properties config = loadProperties("kafka-streams.properties");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamCoreCustomersToIntegration.class.toString());
        config.put(StreamsConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        startCustomerStream(config);
    }

    private static void startCustomerStream(Properties config) {
        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<String, IntPartner> customerStream = builder.stream(INPUT_TOPIC_CUSTOMERS)
                //TODO check what happens for a delete, do we get a null data and this would work deleting the record from a KTable referencing this topic?
                .filterNot((k, v) -> ((CoreCustomer)v).getHeaders().getOperation().equals(operation.DELETE))
                .map((k, v) -> new KeyValue<>(
                        "0_" + ((CoreCustomer)v).getData().getCOCUID(), createPartnerFromCustomer(((CoreCustomer)v).getData())));
        customerStream.to(OUTPUT_TOPIC_PARTNERS);

        final KafkaStreams streamsCustomers = new KafkaStreams(builder, config);

        streamsCustomers.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streamsCustomers::close));
    }

    private static IntPartner createPartnerFromCustomer(CoreCustomerRecord customer) {
        List<Address> addresses = new ArrayList<>();
        addresses.add(Address.newBuilder()
                .setStreet(customer.getCOCUSTREET())
                .setHouseNumber(customer.getCOCUHOUSENR())
                .setPostalCode(customer.getCOCUPOSTALCODE())
                .setCity(customer.getCOCUCITY())
                .build());

        Object personalData;
        if (customer.getCOCUTYP()==1) {
            personalData = NaturalPerson.newBuilder()
                    .setFamilyName(customer.getCOCUFAMILYNAME())
                    .setName(customer.getCOCUNAME())
                    .setBirthdate(customer.getCOCUBIRTHDATE())
                    .setGender(customer.getCOCUGENDER().equals("M") ? Gender.M : Gender.F)
                    .build();
        }
        else {
            personalData = LegalEntity.newBuilder()
                    .setCompanyName(customer.getCOCUCOMPANYNAME())
                    .build();
        }

        return IntPartner.newBuilder()
                .setId("0_" + customer.getCOCUID())
                .setAddresses(addresses)
                .setPersonalData(personalData)
                .build();

    }

}
