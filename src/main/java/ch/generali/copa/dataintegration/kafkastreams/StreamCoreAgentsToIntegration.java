package ch.generali.copa.dataintegration.kafkastreams;

import avro.shaded.com.google.common.collect.ImmutableMap;
import ch.generali.copa.dataintegration.kafkastreams.integration.*;
import ch.generali.copa.dataintegration.kafkastreams.landing.Agent.*;
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
public class StreamCoreAgentsToIntegration {

    private static final String INPUT_TOPIC_AGENTS = "CORE_AGENTS";
    private static final String OUTPUT_TOPIC_PARTNERS = "INTE_PARTNERS";

    static public void main(String[] args) {

        Properties config = loadProperties("kafka-streams.properties");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamCoreAgentsToIntegration.class.toString());
        config.put(StreamsConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        startAgentStream(config);
    }

    private static void startAgentStream(Properties config) {
        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<String, IntPartner> agentStream = builder.stream(INPUT_TOPIC_AGENTS)
                //TODO check what happens for a delete, do we get a null data and this would work deleting the record from a KTable referencing this topic?
                .filterNot((k, v) -> ((CoreAgent)v).getHeaders().getOperation().equals(operation.DELETE))
                .map((k, v) -> new KeyValue<>(
                        "1_" + ((CoreAgent)v).getData().getCOAGID(), createPartnerFromAgent(((CoreAgent)v).getData())));
        agentStream.to(OUTPUT_TOPIC_PARTNERS);

        final KafkaStreams streamsAgents = new KafkaStreams(builder, config);

        streamsAgents.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streamsAgents::close));
    }

    private static IntPartner createPartnerFromAgent(CoreAgentRecord agent) {
        List<Address> addresses = new ArrayList<>();
        addresses.add(Address.newBuilder()
            .setStreet(agent.getCOAGSTREET())
            .setHouseNumber(agent.getCOAGHOUSENR())
            .setPostalCode(agent.getCOAGPOSTALCODE())
            .setCity(agent.getCOAGCITY())
            .setCountry(convertCountry(agent.getCOAGCOUNTRY()))
            .build());

        NaturalPerson personalData = NaturalPerson.newBuilder()
                .setFamilyName(agent.getCOAGFAMILYNAME())
                .setName(agent.getCOAGNAME())
                .setBirthdate(LocalDate.parse(agent.getCOAGBIRTHDATE(), DateTimeFormat.forPattern("dd.MM.yyyy")))
                .setGender(agent.getCOAGGENDER().equals("M")? Gender.M : Gender.F)
                .build();

        System.out.printf("Got date %s", agent.getCOAGBIRTHDATE());

        return IntPartner.newBuilder()
                .setId("1_" + agent.getCOAGID())
                .setAgent(ch.generali.copa.dataintegration.kafkastreams.integration.Agent.newBuilder()
                    .setActive(agent.getCOAGACTIVE() == 1)
                    .build())
                .setAddresses(addresses)
                .setPersonalData(personalData)
                .build();
    }

    private static Country convertCountry(Integer country) {
        Map<Integer, Country> countryMap = ImmutableMap.of(
            1, Country.CH,
            2, Country.DE,
            3, Country.FR,
            4, Country.IT,
            5, Country.AT);
        return countryMap.get(country);
    }


}
