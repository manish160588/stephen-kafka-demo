package io.kafka.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        //Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        properties.setProperty("batch.size","400");
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        for(int j=0;j<10;j++) {
            for (int i = 0; i < 30; i++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world" + i);

                //send data -- asynchronous
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        //executed everytime a record is successfully sent or an exception is thrown
                        if (e == null) {
                            //record was successfully sent
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "partition: " + metadata.partition() + "\n" +
                                    "Offsets: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp());
                        } else {
                            log.error("Exception occured {}", e);
                        }
                    }
                });
            }
            // Create a producer record
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //flush and close the producer
        // tell the producer to send all data and block until done - synchronous
        producer.flush();

        //flush and close the producer
        producer.close();
    }
}
