package com.epam.bigdata.task7;

import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

/**
 * Created by Aliaksei_Neuski on 10/7/16.
 */
public class Consumer {

    public static void main(String[] args) throws IOException {

        final String topic = args[1];
        final String path = args[2];

        KafkaConsumer<String, String> consumer;
        try (InputStream props = Resources.getResource("consumer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            consumer = new KafkaConsumer<>(properties);
        }

        consumer.subscribe(Collections.singletonList(topic));
        int timeouts = 0;

        //noinspection InfiniteLoopStatement
        do {
            ConsumerRecords<String, String> records = consumer.poll(200);
            if (records.count() == 0) {
                ++timeouts;
            }
            else {
                System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                timeouts = 0;
            }

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.toString());
            }
        } while (true);
    }
}
