package playground.kanand.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerDemoWithKeys {

    public static final String SEP = "\t|";
    static Logger logger   = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
    public static void main(String[] args) {
        System.out.println("KafKA : Producer with Keys");

        // producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer  = new KafkaProducer<String, String>(properties);

        IntStream.range(0, 10).forEach( i -> sendMsg(producer, i));

        producer.flush();
        producer.close();
    }

    /**
     *
     * @param producer
     * @param i
     */
    private static void sendMsg(KafkaProducer<String, String> producer, int i) {
        String topic    = "first_topic";
        String msg      = "Java : " + i;
        String key      = "id_" + i;
        // create producer record
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, msg);

        logger.info("Key :" + key);

        // send data - async
        try {
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // exec on msg successful sent
                    if ( e == null) {
                        logger.info("Received new Record Metadata " + SEP +
                                "Topic:" + recordMetadata.topic()  + SEP +
                                "Key:" + recordMetadata.topic()  + SEP +
                                "Partition:" + recordMetadata.partition() + SEP +
                                "Offset:" + recordMetadata.offset() + SEP +
                                "Timestamp:" + recordMetadata.timestamp() );
                    } else {
                        logger.error("ERROR Record Metadata " + SEP +
                                "Topic:" + recordMetadata.topic()  + SEP +
                                "Partition:" + recordMetadata.partition() + SEP +
                                "Offset:" + recordMetadata.offset() + SEP +
                                "Timestamp:" + recordMetadata.timestamp() , e);
                    }
                }
            }).get();   // get makes call synchronous , AVOID in prod
        } catch ( Exception ex) {
            logger.error("Exception Occurred", ex);
        }

    }
}
