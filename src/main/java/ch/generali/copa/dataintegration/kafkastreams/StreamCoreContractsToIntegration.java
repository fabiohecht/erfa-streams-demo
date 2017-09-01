package ch.generali.copa.dataintegration.kafkastreams;

import avro.shaded.com.google.common.collect.ImmutableMap;
import ch.generali.copa.dataintegration.kafkastreams.integration.*;
import ch.generali.copa.dataintegration.kafkastreams.landing.Contract.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;

import java.util.*;

import static ch.generali.copa.dataintegration.kafkastreams.KafkaStreamWorker.loadProperties;

/**
 * Created by fabio on 8/31/17.
 */
public class StreamCoreContractsToIntegration {

    private static final String INPUT_TOPIC_CONTRACTS = "CORE_CONTRACTS";
    private static final String OUTPUT_TOPIC_CONTRACTS = "INTE_CONTRACTS";

    static public void main(String[] args) {

        Properties config = loadProperties("kafka-streams.properties");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamCoreContractsToIntegration.class.toString());
        config.put(StreamsConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        startContractStream(config);
    }

    private static void startContractStream(Properties config) {
        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<String, IntContract> contractStream = builder.stream(INPUT_TOPIC_CONTRACTS)
                //TODO check what happens for a delete, do we get a null data and this would work deleting the record from a KTable referencing this topic?
                .filterNot((k, v) -> ((CoreContract)v).getHeaders().getOperation().equals(operation.DELETE))
                .map((k, v) -> new KeyValue<>(
                        ((CoreContract)v).getData().getCOCOID().toString(), createPartnerFromContract(((CoreContract)v).getData())));
        contractStream.to(OUTPUT_TOPIC_CONTRACTS);

        final KafkaStreams streamsContracts = new KafkaStreams(builder, config);

        streamsContracts.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streamsContracts::close));
    }

    private static IntContract createPartnerFromContract(CoreContractRecord contract) {

        return IntContract.newBuilder()
                .setId(contract.getCOCOID().toString())
                .setType(convertType(contract.getCOCOTYPE()))
                .setCoverage(contract.getCOCOCOVERAGE())
                .setAnnualPremium(contract.getCOCOANNUALPREMIUM())
                .setStartDate(contract.getCOCOSTARTDATE())
                .setEndDate(contract.getCOCOENDDATE())
                .setPartnerIdCustomer("0_" + contract.getCOCOCOCUID())
                .setPartnerIdAgent("1_" + contract.getCOCOCOAGID())
                .setRentability(contract.getCOCOANNUALPREMIUM() - contract.getCOCOTOTALPAIDCLAIMS())
                .build();
    }


    private static ContractType convertType(Integer type) {
        Map<Integer, ContractType> map = ImmutableMap.of(
                1, ContractType.life,
                2, ContractType.car,
                3, ContractType.house,
                4, ContractType.legal);
        return map.get(type);
    }
}
