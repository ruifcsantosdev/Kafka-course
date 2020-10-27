package com.github.ruifcsantos59.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("Producer APP");

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        // create the producer properties
        // Config Documentation in: https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);




        for(int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "Hello world producer demo JAVA " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            /*
            * key 0 part 1
            * key 1 part 0
            * key 2 part 2
            * key 3 part 0
            * key 4 part 2
            * key 5 part 2
            * key 6 part 0
            * key 7 part 2
            * key 8 part 1
            * key 9 part 2
            * */

            // create a producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key);

            // send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if(e == null) {
                        // the record was successfully sent
                        logger.info("Received new metadata \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    }else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); //block the .send(), don't do this in production!
        }


        producer.flush();

        producer.close();
    }
}
