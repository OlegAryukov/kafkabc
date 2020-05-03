package ru.aryukov.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);
        String bootstarServer = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstarServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        for (int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String value = "Value" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            System.out.println("Key " + key);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every timearecord is succes
                    if (e == null) {
                        System.out.println("Metadate \n" +
                                "Topic:" + recordMetadata.topic()
                                + "\n Partition:" + recordMetadata.partition()
                                + "\n Offset:" + recordMetadata.offset());
                    } else {
                        System.out.println("Error" + e);
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
