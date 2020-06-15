package playground.kanand.tutorial2;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

public class TwitterClient {
    public static final String TOPIC = "news_tweets";
    public static final int PAGE_MSG_FETCH = 200;
    static org.slf4j.Logger logger   = LoggerFactory.getLogger(TwitterClient.class);
    /**
     * Usage: java twitter4j.examples.timeline.GetHomeTimeline
     *
     * @param args String[]
     */
    public static void main(String[] args) {
        String[] channels = {"CNN", "ANI", "WIRED", "CNBC", "nytimes"};
        try {
            // gets Twitter instance with default credentials
            List<Status> statuses = new ArrayList<>();
            fetchTweets(channels, statuses);
            KafkaProducer<String, String> producer   = createProducer();
            String content;
            Gson gson = new Gson();

            for (Status status : statuses) {
//                content = "@" + status.getUser().getScreenName() + " - " + status.getText();
                content = gson.toJson(status);
                logger.info(content);
                sendMsg(producer, String.valueOf(status.getId()), content);
            }

            producer.close(Duration.ofSeconds(60));

        } catch (TwitterException te) {
            te.printStackTrace();
            logger.warn("Failed to get timeline: " + te.getMessage(),te);
            System.exit(-1);
        }
    }

    private static KafkaProducer<String, String> createProducer() {
        // producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // high throughput
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); //
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32 * 1024)); // 32KB


        // create producer
        return new KafkaProducer<String, String>(properties);
    }

    private static void fetchTweets(String[] channels, List<Status> statuses) throws TwitterException {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setJSONStoreEnabled(false);

        Twitter twitter = new TwitterFactory(cb.build()).getInstance();
//            User user = twitter.verifyCredentials();
//            logger.info("Showing @" + user.getScreenName() + "'s home timeline.");
//            List<Status> statuses = twitter.getHomeTimeline();
        Paging paging   = new Paging();
        paging.setCount(PAGE_MSG_FETCH);
        Stream.of(channels).forEach(ch -> {
            try {
                statuses.addAll(twitter.getUserTimeline(ch, paging));
                Map<String, RateLimitStatus> rateLimitStatus = twitter.getRateLimitStatus();
                logger.info(rateLimitStatus.get("/statuses/user_timeline").toString());
            } catch (TwitterException e) {
                logger.warn("Error while fetching tweets",e);
            }
        });
    }

    /**
     *
     * @param producer
     * @param msg
     */
    private static void sendMsg(KafkaProducer<String, String> producer, String key, String msg) {
        // create producer record
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, msg);

        // send data - async
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // exec on msg successful sent
                if (e == null) {
                    logger.info("Received new Record Metadata \n" +
                            "Topic:" + recordMetadata.topic() + '\n' +
                            "Partition:" + recordMetadata.partition() + '\n' +
                            "Offset:" + recordMetadata.offset() + '\n' +
                            "Timestamp:" + recordMetadata.timestamp());
                } else {
                    logger.error("ERROR Record Metadata \n" +
                            "Topic:" + recordMetadata.topic() + '\n' +
                            "Partition:" + recordMetadata.partition() + '\n' +
                            "Offset:" + recordMetadata.offset() + '\n' +
                            "Timestamp:" + recordMetadata.timestamp(), e);
                }
            }
        });
    }
}
