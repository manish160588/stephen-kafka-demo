package io.kafka.demo.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiMediaChangeHandler implements EventHandler {

    private final KafkaProducer<String, String> kafkaProducer;
    String topic ;

    private final Logger log = LoggerFactory.getLogger(WikiMediaChangeHandler.class.getSimpleName());

    public WikiMediaChangeHandler(KafkaProducer<String,String> kafkaProducer, String topic){
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }
    @Override
    public void onOpen() {
        //nothing here to do
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
        // aysnchronous
        kafkaProducer.send(new ProducerRecord<>(topic,messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) {
        //nothing here
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in Stream Reading",t);
    }
}
