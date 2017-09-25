package ch.ipt.erfakafkastreams;

import ch.ipt.kafkastreamsdemo.Interest;
import com.eneco.trading.kafka.connect.twitter.Tweet;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by fabio on 8/31/17.
 */
public class StreamTwitter extends AbstractStream {

    private static final String INPUT_TOPIC_TWITTER = "twitter";
    private static final String INPUT_TOPIC_CUSTOMER = "customer";
    private static final String INPUT_TOPIC_KEYWORD = "keyword";

    static public void main(String[] args) {
        initializeConfig();
        startContractStream();
    }

    private static void startContractStream() {
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
}