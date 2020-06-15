package playground.kanand.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static final String SEP = "\t|";
    static Logger log = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().execute();
    }

    public ConsumerDemoWithThreads () {
        // constructor
    }

    private void execute() {
        String bootstrapServers = "127.0.0.1:9092";
        String groupID  = "app5_Java";
        String topic  = "first_topic";

        // Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        CountDownLatch countDownLatch   = new CountDownLatch(1);

        // Create Consumer Runnable
        log.info("Creating the consumer Thread");
        ConsumerRunnable con1  = new ConsumerRunnable(properties, topic, countDownLatch);
        Thread thread1 = new Thread(con1);

        thread1.start();

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught Shutdown Hook");
            con1.shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Application has exited");
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error("Thread Interrupted !!", e);
        } finally {
            log.info("Application is closing");
        }
    }
}

class ConsumerRunnable implements Runnable {
    public static final String SEP = "\t|";

    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;
    private Logger log  = LoggerFactory.getLogger(ConsumerRunnable.class);

    public ConsumerRunnable(Properties properties, String topic, CountDownLatch latch) {
        this.latch  = latch;

        // create consumer
        consumer  = new KafkaConsumer<>(properties);

        // subscribe consumer
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        try {
            // poll for new Data
            while (true) {
                ConsumerRecords<String, String> records   = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key :" + record.key() + SEP + "Value [" + record.value() + "] \t- (Partition : "+ record.partition() + SEP + "Offset :" + record.offset() + ")");
                }
            }
        } catch (WakeupException wex) {
            log.info("Shutdown Signal received");
        } finally {
            consumer.close();
            latch.countDown();  // release
        }
    }

    public void shutdown() {
        // wakeup interrupts consumer poll, throws WakeUpException
        consumer.wakeup();
    }
}
