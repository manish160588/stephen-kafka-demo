package io.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");

        String group_id="my.java.application";
        String topic = "demo_java";

        // Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        // Create Consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());

        properties.setProperty("group.id",group_id);

        properties.setProperty("auto.offset.reset","earliest");  //can be value either of none/earliest/latest

        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
       // properties.setProperty("group.instance.id",".."); //strategy for static assignment

        //create consumer
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        //get the referemce to the  thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hok
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Detected a shutdown, lets exit by calling consumer.wakeup()...");
                kafkaConsumer.wakeup();

                // join the main thread to allow the execution of code in main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            //subscribe to a topic
            kafkaConsumer.subscribe(Arrays.asList(topic));

            //poll for data
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : consumerRecords) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        }catch (WakeupException e){
            log.info("Consumer is starting to shut down");
        }catch (Exception e){
            log.error("unexpected exception in the consumer",e);
        }finally {
            kafkaConsumer.close(); //close the consumer, will also commit offsets
            log.info("The consumer is shutdown gracefully");
        }
    }
}
