package io.kafka.demo.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleBiFunction;

public class WikiMediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {

        String bootstrapServer = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia_recentchange";

        EventHandler eventHandler = new WikiMediaChangeHandler(producer,topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        //start the producer in another thread
        eventSource.start();

        // we produce for 10 min and then block the program until then
        TimeUnit.MINUTES.sleep(10);
    }
}
