package playground.kanand.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static final String SEP = "\t|";
    static Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupID  = "app4_Java";
        String topic  = "first_topic";

        // Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer  = new KafkaConsumer<>(properties);

        // subscribe consumer
        consumer.subscribe(Arrays.asList(topic));

        // poll for new Data
        while (true) {
            ConsumerRecords<String, String> records   = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Key :" + record.key() + SEP + "Value [" + record.value() + "] \t- (Partition : "+ record.partition() + SEP + "Offset :" + record.offset() + ")");
            }
        }
    }
}
