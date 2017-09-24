package ch.ipt.erfakafkastreams;

import ch.ipt.kafkastreamsdemo.Interest;
import com.eneco.trading.kafka.connect.twitter.Tweet;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.joda.time.LocalDate;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by fabio on 8/31/17.
 */
public class StreamTwitter {

    private static final String INPUT_TOPIC_TWITTER = "twitter";
    private static final String INPUT_TOPIC_CUSTOMER = "customer";
    private static final String INPUT_TOPIC_KEYWORD = "keyword";

    static public void main(String[] args) {

        Properties config = loadProperties("kafka-streams.properties");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamTwitter.class.getName() + "2");
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

        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        final KStream<String, Tweet> twitterStream = builder.stream(INPUT_TOPIC_TWITTER);
        final KTable<String, String> customerTable = builder.table(new Serdes.StringSerde(), new Serdes.StringSerde(), INPUT_TOPIC_CUSTOMER);
        final KTable<String, String> keywordTable = builder.table(new Serdes.StringSerde(), new Serdes.StringSerde(), INPUT_TOPIC_KEYWORD).mapValues(value -> value.toLowerCase());

        final KStream<String, Interest> interestStream = twitterStream
                // key needs to be user name to join with customerTable
                .map((k, v) -> new KeyValue<>(v.getUser().getScreenName(), Interest.newBuilder()
                        .setTweet(v.getText())
                        .setTimestamp(ISODateTimeFormat.dateTime().parseDateTime(v.getCreatedAt()))
                        .setUser(v.getUser().getScreenName())
                        .setCustomer("") //will be filled later
                        .setTopic("") //will be filled later
                        .build()))
                // join with customerTable
                .join(
                        customerTable,
                        (interest, customer) -> {
                            interest.setCustomer(customer);
                            return interest;
                        }
                )
                // key needs to be word to join with keyword
                .flatMap((k, v) ->
                {
                    List<KeyValue<String, Interest>> result = new LinkedList<>();
                    Arrays.stream(pattern.split(v.getTweet().toLowerCase())).forEach(s -> result.add(KeyValue.pair(s, v)));
                    return result;
                })
                .join(
                        keywordTable,
                        (interest, topic) -> {
                            interest.setTopic(topic);
                            return interest;
                        }
                )
                .peek(
                        (keyword, interest) -> System.out.printf("new interest: %s\n", interest.toString())
                );
        interestStream.to("interest");

        final KafkaStreams streamsContracts = new KafkaStreams(builder, config);

        streamsContracts.cleanUp();
        streamsContracts.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streamsContracts::close));
    }

    protected static Properties loadProperties(String filename) {

        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = StreamTwitter.class.getClassLoader().getResourceAsStream(filename);
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
