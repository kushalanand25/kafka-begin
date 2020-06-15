package playground.kanand.tutorial4;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ElasticSearchConsumer {
    private static final Logger logger   = LoggerFactory.getLogger(ElasticSearchConsumer.class);
    public static final String TOPIC    = "news_tweets";
    public static final String SEP      = "\t|";

    private static RestHighLevelClient createClient() {
        String username = "1ieychyn4k";
        String password = "odba3rdt1d";
        String hostname = "twitter-feed-6070889479.eu-west-1.bonsaisearch.net";

        final CredentialsProvider credentialProvider    = new BasicCredentialsProvider();
        credentialProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialProvider);
                    }
                })
                .setRequestConfigCallback(
                    new RestClientBuilder.RequestConfigCallback() {
                    @Override
                    public RequestConfig.Builder customizeRequestConfig(
                            RequestConfig.Builder requestConfigBuilder) {
                        return requestConfigBuilder
                                .setConnectTimeout(5000)
                                .setSocketTimeout(60000);
                    }
                }));

        return client;
    }

    private static KafkaConsumer<String, String> createConsumer (String topic) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupID  = "tweet_feed_elasticsearch";

        // Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(10 * 60 * 1000));

        // create consumer
        KafkaConsumer<String, String> consumer  = new KafkaConsumer<>(properties);

        // subscribe consumer
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    public static void main(String[] args) {
        String tweetJson    = getTweet();

        try (RestHighLevelClient restHighLevelClient = createClient();
             KafkaConsumer<String, String> consumer  = createConsumer(TOPIC)) {

            int recordCount;
            int retries = 0;
            BulkRequest bulkRequest;
            BulkResponse bulkResponse;
            IndexRequest indexRequest;
            while (true) {
                ConsumerRecords<String, String> records   = consumer.poll(Duration.ofMillis(100));
                recordCount = records.count();
                bulkRequest = new BulkRequest();
                logger.info("Received {} records", recordCount);
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key :" + record.key() + SEP + "(Partition : "+ record.partition() + SEP + "Offset :" + record.offset() + ")");
//                    logger.info("text : {}", record.value());
//                    putTweet(record.key(), record.value(), restHighLevelClient);

                    indexRequest = new IndexRequest("tw")
                            .id(record.key())
                            .source(tweetJson, XContentType.JSON);

                    bulkRequest.add(indexRequest);
                }

                if (recordCount > 0)  {
                    retries = 0;
                    bulkResponse    = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info("Committing offset ....");
                    consumer.commitSync(Duration.ofSeconds(2));
                    logger.info("Offset committed !!!");
                } else {
                    retries++;
                    if (retries > 10)
                        break;
                }
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
        }
    }

    private static void putTweet(String id, String tweetJson, RestHighLevelClient restHighLevelClient) throws IOException {
        IndexRequest indexRequest = new IndexRequest("tw")
                .id(id)
                .source(tweetJson, XContentType.JSON);

        IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        String indexedId = response.getId();

        logger.info("indexed - id : {}", indexedId);
    }

    private static String getTweet() {
        return "{\n" +
                "        \"created_at\": \"Tue Jun 02 15:47:29 +0000 2020\",\n" +
                "        \"id\": 1267845299407474693,\n" +
                "        \"id_str\": \"1267845299407474693\",\n" +
                "        \"text\": \"@KanandSocialite Whats upp Bro\",\n" +
                "        \"truncated\": false,\n" +
                "        \"entities\": {\n" +
                "            \"hashtags\": [],\n" +
                "            \"symbols\": [],\n" +
                "            \"user_mentions\": [\n" +
                "                {\n" +
                "                    \"screen_name\": \"KanandSocialite\",\n" +
                "                    \"name\": \"kanand.socialite\",\n" +
                "                    \"id\": 993791947700453376,\n" +
                "                    \"id_str\": \"993791947700453376\",\n" +
                "                    \"indices\": [\n" +
                "                        0,\n" +
                "                        16\n" +
                "                    ]\n" +
                "                }\n" +
                "            ],\n" +
                "            \"urls\": []\n" +
                "        },\n" +
                "        \"source\": \"<a href=\\\"https://mobile.twitter.com\\\" rel=\\\"nofollow\\\">Twitter Web App</a>\",\n" +
                "        \"in_reply_to_status_id\": 1267801252236128263,\n" +
                "        \"in_reply_to_status_id_str\": \"1267801252236128263\",\n" +
                "        \"in_reply_to_user_id\": 993791947700453376,\n" +
                "        \"in_reply_to_user_id_str\": \"993791947700453376\",\n" +
                "        \"in_reply_to_screen_name\": \"KanandSocialite\",\n" +
                "        \"user\": {\n" +
                "            \"id\": 1260998857145511937,\n" +
                "            \"id_str\": \"1260998857145511937\",\n" +
                "            \"name\": \"KUSHAL ANAND\",\n" +
                "            \"screen_name\": \"kushal_anand_1\",\n" +
                "            \"location\": \"\",\n" +
                "            \"description\": \"\",\n" +
                "            \"url\": null,\n" +
                "            \"entities\": {\n" +
                "                \"description\": {\n" +
                "                    \"urls\": []\n" +
                "                }\n" +
                "            },\n" +
                "            \"protected\": false,\n" +
                "            \"followers_count\": 1,\n" +
                "            \"friends_count\": 15,\n" +
                "            \"listed_count\": 0,\n" +
                "            \"created_at\": \"Thu May 14 18:22:26 +0000 2020\",\n" +
                "            \"favourites_count\": 0,\n" +
                "            \"utc_offset\": null,\n" +
                "            \"time_zone\": null,\n" +
                "            \"geo_enabled\": false,\n" +
                "            \"verified\": false,\n" +
                "            \"statuses_count\": 1,\n" +
                "            \"lang\": null,\n" +
                "            \"contributors_enabled\": false,\n" +
                "            \"is_translator\": false,\n" +
                "            \"is_translation_enabled\": false,\n" +
                "            \"profile_background_color\": \"F5F8FA\",\n" +
                "            \"profile_background_image_url\": null,\n" +
                "            \"profile_background_image_url_https\": null,\n" +
                "            \"profile_background_tile\": false,\n" +
                "            \"profile_image_url\": \"http://abs.twimg.com/sticky/default_profile_images/default_profile_normal.png\",\n" +
                "            \"profile_image_url_https\": \"https://abs.twimg.com/sticky/default_profile_images/default_profile_normal.png\",\n" +
                "            \"profile_link_color\": \"1DA1F2\",\n" +
                "            \"profile_sidebar_border_color\": \"C0DEED\",\n" +
                "            \"profile_sidebar_fill_color\": \"DDEEF6\",\n" +
                "            \"profile_text_color\": \"333333\",\n" +
                "            \"profile_use_background_image\": true,\n" +
                "            \"has_extended_profile\": false,\n" +
                "            \"default_profile\": true,\n" +
                "            \"default_profile_image\": true,\n" +
                "            \"following\": false,\n" +
                "            \"follow_request_sent\": false,\n" +
                "            \"notifications\": false,\n" +
                "            \"translator_type\": \"none\"\n" +
                "        },\n" +
                "        \"geo\": null,\n" +
                "        \"coordinates\": null,\n" +
                "        \"place\": null,\n" +
                "        \"contributors\": null,\n" +
                "        \"is_quote_status\": false,\n" +
                "        \"retweet_count\": 0,\n" +
                "        \"favorite_count\": 0,\n" +
                "        \"favorited\": false,\n" +
                "        \"retweeted\": false,\n" +
                "        \"lang\": \"en\"\n" +
                "    }";
    }
}
