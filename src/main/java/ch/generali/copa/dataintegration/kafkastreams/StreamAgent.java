package ch.generali.copa.dataintegration.kafkastreams;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by fabio on 8/24/17.
 */
public class StreamAgent {

    private static final String INPUT_TOPIC = "CORE_AGENTS";
    private static final String OUTPUT_TOPIC_AGENTS = "INTE_AGENTS";
    private static final String OUTPUT_TOPIC_NATURAL_PERSONS = "INTE_NATURAL_PERSONS";
    private static final String OUTPUT_TOPIC_PARTNERS = "INTE_PARTNERS";

    static public void main(String[] args) {

        Properties props = loadProperties("kafka-streams.properties");

        System.out.println(props);
        System.out.println(props.stringPropertyNames());
        KStreamBuilder builder = new KStreamBuilder();
/*
        final Serde<String> stringSerde = Serdes.String();
        //final Serde<byte[]> byteArraySerde = Serdes.ByteArray();
        final KStream<byte[], String> textLines = builder.stream(stringSerde, stringSerde, INPUT_TOPIC);
        final KStream<byte[], String> uppercasedWithMapValues = textLines.mapValues(String::toUpperCase);
        uppercasedWithMapValues.to("UppercasedTextLinesTopic");

        System.out.println("fullStream");
        KStream<String, String> fullStream = builder.stream("co_full_out");
        fullStream.print();

        //KTable<String, Long> wordCounts = textLines.countByKey("fieldId");
        //wordCounts.to("another_topic");
        System.out.println("KafkaStreams");
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
*/


    }

    static private Properties loadProperties(String filename) {

        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = StreamAgent.class.getClassLoader().getResourceAsStream(filename);
            if (input == null) {
                throw new RuntimeException("Unable to find configuration file " + filename);
            }

            //load a properties file from class path, inside static method
            prop.load(input);

            return prop;

        } catch (IOException e) {
            throw new RuntimeException("Unable to read configuration file " + filename, e);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    throw new RuntimeException("Unable to read configuration file " + filename, e);
                }
            }
        }

    }

}
