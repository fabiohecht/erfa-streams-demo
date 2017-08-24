package ch.generali.copa.dataintegration.kafkastreams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by fabio on 8/24/17.
 */
public class StreamAgent {

    static public void main(String[] args) {

        Properties props = loadProperties("kafka-streams.properties");


        final KStream<Integer, Integer> input = builder.stream(INPUT_TOPIC);
        final KTable<Integer, Integer> sumOfOddNumbers = input
                // We are only interested in odd numbers.
                .filter((k, v) -> v % 2 != 0)
                // We want to compute the total sum across ALL numbers, so we must re-key all records to the
                // same key.  This re-keying is required because in Kafka Streams a data record is always a
                // key-value pair, and KStream aggregations such as `reduce` operate on a per-key basis.
                // The actual new key (here: `1`) we pick here doesn't matter as long it is the same across
                // all records.
                .selectKey((k, v) -> 1)
                // no need to specify explicit serdes because the resulting key and value types match our default serde settings
                .groupByKey()
                // Add the numbers to compute the sum.
                .reduce((v1, v2) -> v1 + v2, "sum");
        sumOfOddNumbers.to(SUM_OF_ODD_NUMBERS_TOPIC);

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        final KafkaStreams streams = createStreams(props);
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
