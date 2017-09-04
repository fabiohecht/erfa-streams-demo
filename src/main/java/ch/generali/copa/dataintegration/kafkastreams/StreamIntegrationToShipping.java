package ch.generali.copa.dataintegration.kafkastreams;

import ch.generali.copa.dataintegration.kafkastreams.integration.IntContract;
import ch.generali.copa.dataintegration.kafkastreams.integration.IntPartner;
import ch.generali.copa.dataintegration.kafkastreams.shipping.ShipCustomersOverview;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.util.*;

import static ch.generali.copa.dataintegration.kafkastreams.KafkaStreamWorker.loadProperties;

/**
 * Created by fabio on 8/31/17.
 */
public class StreamIntegrationToShipping {

    private static final String INPUT_TOPIC_CONTRACTS = "INTE_CONTRACTS";
    private static final String INPUT_TOPIC_PARTNERS = "INTE_PARTNERS";
    private static final String OUTPUT_TOPIC_SHIPPING = "SHIP_CUSTOMERS_OVERVIEW";

    static public void main(String[] args) {

        Properties config = loadProperties("kafka-streams.properties");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamIntegrationToShipping.class.getName()+"9");
        config.put(StreamsConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        startContractStream(config);
    }

    private static void startContractStream(Properties config) {
        final KStreamBuilder builder = new KStreamBuilder();

        // create store
        StateStoreSupplier myStore = Stores.create("myTransformState")
                .withStringKeys()
                .withLongValues()
                .persistent() // optional
                .build();
        builder.addStateStore(myStore);

        final KTable<String, IntPartner> partnersTable = builder.table(INPUT_TOPIC_PARTNERS);

        final KStream<String, IntContract> contractsStream = builder.stream(INPUT_TOPIC_CONTRACTS);
        final KGroupedStream<String, IntContract> contractsGroupedByCustomer = contractsStream
                .peek((key, value) -> System.out.println("in contractsStream " + key + " => " + value))
                .selectKey((k,v) -> v.getPartnerIdCustomer())
                .peek((key, value) -> System.out.println("in contractsStream NK " + key + " => " + value))
                .groupByKey();

        final KTable<String, Long> countContractsPerCustomer = contractsGroupedByCustomer.count();
        final KTable<String, Long> sumRentabilityPerCustomer = contractsGroupedByCustomer.aggregate(
                () -> 0L,
                (key, value, aggregate) -> (long)value.getRentability() + aggregate,
                Serdes.Long());
        final KTable<String, Long> countAgentsPerCustomer = contractsStream
                .filter((k, v) -> v.getPartnerIdAgent() != null)
                .map((k,v) -> KeyValue.pair(v.getPartnerIdCustomer(), v/*.getPartnerIdAgent()*/))
                .groupByKey()
                .count();
        //TODO filter by email and phone number (not yet modeled)
        final KTable<String, IntPartner> customersWithContactData = partnersTable
                //partners that are customers (not agent)
//                .filter((k, v) -> v.getAgent() != null)
                ;

//        countContractsPerCustomer.to(new Serdes.StringSerde(), new Serdes.LongSerde(), "debug_countContractsPerCustomer");
//        sumRentabilityPerCustomer.to(new Serdes.StringSerde(), new Serdes.LongSerde(),"debug_sumRentabilityPerCustomer");
//        countAgentsPerCustomer.to(new Serdes.StringSerde(), new Serdes.LongSerde(),"debug_countAgentsPerCustomer");
//        customersWithContactData.to("debug_customersWithContactData");
        //all on this table are VIPs, non-VIPs are not here
        final KTable<String, Long> vipCustomers = contractsStream
                .selectKey((k,v) -> v.getPartnerIdCustomer())
                .join(
                        customersWithContactData,
                        (contract, customer) -> contract.getRentability()
                )
                .filter((k, rentability) -> rentability >= 0)
                .groupByKey()
                .count();

        final KTable<String, ShipCustomersOverview> shipping = countContractsPerCustomer
                .mapValues((v) -> {
                    ShipCustomersOverview ship = new ShipCustomersOverview();
                    ship.setNumberOfContracts(v);

                    return ship;
                })
                .join(
                        customersWithContactData,
                        (ship, customer) -> {
                            System.out.println("customersWithContactData will join "+ship+" with "+ customer);
                            ship.setId(customer.getId());
                            return ship;
                        }
                )
                .leftJoin(
                        sumRentabilityPerCustomer,
                        (ship, rentability) -> {
                            System.out.println("sumRentabilityPerCustomer will join "+ship+" with "+ rentability);
                            ship.setTotalRentability(rentability);
                            return ship;
                        }
                )
                .leftJoin(
                        countAgentsPerCustomer,
                        (ship, numAgents) -> {
                            if (numAgents != null) {
                                ship.setNumberOfAgents(numAgents);
                                ship.setVip(true); //TODO fix below
                            }
                            return ship;
                        }
                )
                .leftJoin(
                        vipCustomers,
                        (ship, vipCustomer) -> {
                            ship.setVip(true);
                            return ship;
                        }
                )
        ;

        shipping.to(OUTPUT_TOPIC_SHIPPING);

        final KafkaStreams streamsContracts = new KafkaStreams(builder, config);


        streamsContracts.cleanUp();

        streamsContracts.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streamsContracts::close));
    }

}
