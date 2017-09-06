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
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamIntegrationToShipping.class.getName()+"24");
        config.put(StreamsConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Enable record cache of size 10 MB.
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        // Set commit interval to 1 second.
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

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
        final KTable<String, IntContract> contractsTable = builder.table(INPUT_TOPIC_CONTRACTS);

        final KGroupedTable<String, IntContract> contractsGroupedByCustomer = contractsTable
                .groupBy((k, v) -> new KeyValue<>(v.getPartnerIdCustomer(), v));

        final KTable<String, Long> countContractsPerCustomer = contractsGroupedByCustomer.count();
        final KGroupedTable<String, Long> contractRentabilityTable = contractsTable
                .groupBy((k, v) -> new KeyValue<>(v.getPartnerIdCustomer(), new Long(v.getRentability())),
                        new Serdes.StringSerde(), new Serdes.LongSerde());
        final KTable<String, Long> sumRentabilityPerCustomer = contractRentabilityTable
                .reduce(
                        (currentAggregate, newValue) -> currentAggregate + newValue,
                        (currentAggregate, oldValue) -> currentAggregate - oldValue
                );

        final KTable<String, Long> countAgentsPerCustomer = contractsTable
                .filter((k, v) -> v.getPartnerIdAgent() != null)
                .groupBy((k, v) -> new KeyValue<>(v.getPartnerIdCustomer()+"|"+v.getPartnerIdAgent(), 1L),
                        new Serdes.StringSerde(), new Serdes.LongSerde())
                .reduce(
                        (currentAggregate, newValue) -> currentAggregate + 1,
                        (currentAggregate, oldValue) -> currentAggregate - 1
                )
                .filter((key, value) -> value > 0)
                .groupBy((k, v) -> new KeyValue<>(k.split("\\|")[0], v),
                    new Serdes.StringSerde(),
                    new Serdes.LongSerde()
                )
                .count();

        //TODO filter by email and phone number (not yet modeled)
        final KTable<String, IntPartner> customersWithContactData = partnersTable
                //partners that are customers (not agent)
                .filter((k, v) -> v.getAgent() == null);
                //TODO filter on having contact data;

//        countContractsPerCustomer.to(new Serdes.StringSerde(), new Serdes.LongSerde(), "debug_countContractsPerCustomer");
//        sumRentabilityPerCustomer.to(new Serdes.StringSerde(), new Serdes.LongSerde(),"debug_sumRentabilityPerCustomer");
//        countAgentsPerCustomer.to(new Serdes.StringSerde(), new Serdes.LongSerde(),"debug_countAgentsPerCustomer");
//        customersWithContactData.to("debug_customersWithContactData");
        //all on this table are VIPs, non-VIPs are not here
        final KTable<String, Boolean> vipCustomers = contractsTable
                .filter((k, v) -> v.getRentability() > 0)
                .groupBy((k, v) -> new KeyValue<>(v.getPartnerIdCustomer(), v))
                .count()
                .join(
                        customersWithContactData,
                        (contract, customer) -> Boolean.TRUE
                );

        final KTable<String, ShipCustomersOverview> shipping = partnersTable
                //partners that are customers (not agent)
                .filter((k, v) -> v.getAgent() == null)
                .mapValues((v) -> {
                    ShipCustomersOverview ship = new ShipCustomersOverview();
                    ship.setId(v.getId());
                    return ship;
                })
                .leftJoin(
                        countContractsPerCustomer,
                        (ship, count) -> {
                            System.out.println("countContractsPerCustomer will join "+ship+" with "+ count);
                            if (count != null) {
                                ship.setNumberOfContracts(count);
                            }
                            return ship;
                        }
                )
                .join(
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
                            System.out.println("countAgentsPerCustomer will join "+ship+" with "+ numAgents);
                            if (numAgents != null) {
                                ship.setNumberOfAgents(numAgents);
                            }
                            return ship;
                        }
                )
                .leftJoin(
                        vipCustomers,
                        (ship, vipCustomer) -> {
                            System.out.println("vipCustomer will join "+ship+" with "+ vipCustomer);
                            if (vipCustomer!=null) {
                                ship.setVip(true);
                            }
                            return ship;
                        }
                );

        shipping.to(OUTPUT_TOPIC_SHIPPING);

        final KafkaStreams streamsContracts = new KafkaStreams(builder, config);

        streamsContracts.cleanUp();

        streamsContracts.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streamsContracts::close));
    }

}
