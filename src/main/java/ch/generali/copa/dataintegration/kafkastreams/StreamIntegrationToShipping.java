package ch.generali.copa.dataintegration.kafkastreams;

import avro.shaded.com.google.common.collect.ImmutableMap;
import ch.generali.copa.dataintegration.kafkastreams.integration.*;
import ch.generali.copa.dataintegration.kafkastreams.landing.Agent.CoreAgentRecord;
import ch.generali.copa.dataintegration.kafkastreams.landing.Agent.operation;
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
public class StreamIntegrationToShipping {

}
