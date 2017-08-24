package ch.generali.copa.dataintegration.kafkastreams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by fabio on 8/24/17.
 */
public class StreamAgent {

    static public void main(String[] args) {

        Properties props = loadProperties("kafka-streams.properties");

//        final KafkaStreams streams = createStreams(bootstrapServers,
//                schemaRegistryUrl,
//                "/tmp/kafka-streams");
    }

    static private Properties loadProperties(String filename) {

        Properties prop = new Properties();
        InputStream input = null;

        try {

            String filename = "config.properties";
            input = App3.class.getClassLoader().getResourceAsStream(filename);
            if(input==null){
                System.out.println("Sorry, unable to find " + filename);
                return;
            }

            //load a properties file from class path, inside static method
            prop.load(input);

            //get the property value and print it out
            System.out.println(prop.getProperty("database"));
            System.out.println(prop.getProperty("dbuser"));
            System.out.println(prop.getProperty("dbpassword"));

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally{
            if(input!=null){
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }
    }
}
