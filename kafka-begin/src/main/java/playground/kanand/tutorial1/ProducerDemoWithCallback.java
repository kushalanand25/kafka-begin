package playground.kanand.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerDemoWithCallback {

    static Logger logger   = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
    public static void main(String[] args) {
        System.out.println("KafKA : Producer with Callback");

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
        // create producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Java : " + i);

        // send data - async
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // exec on msg successful sent
                if ( e == null) {
                    logger.info("Received new Record Metadata \n" +
                            "Topic:" + recordMetadata.topic()  + '\n' +
                            "Partition:" + recordMetadata.partition() + '\n' +
                            "Offset:" + recordMetadata.offset() + '\n' +
                            "Timestamp:" + recordMetadata.timestamp() );
                } else {
                    logger.error("ERROR Record Metadata \n" +
                            "Topic:" + recordMetadata.topic()  + '\n' +
                            "Partition:" + recordMetadata.partition() + '\n' +
                            "Offset:" + recordMetadata.offset() + '\n' +
                            "Timestamp:" + recordMetadata.timestamp() , e);
                }
            }
        });
    }
}
