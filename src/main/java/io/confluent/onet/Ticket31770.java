package io.confluent.onet;

import M000043.dbo.AGENCES_AGENTS.Envelope;
import M000043.dbo.AGENCES_AGENTS.Key;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.StreamSupport;

import static java.lang.System.getProperty;
import static java.util.Collections.singletonList;

public class Ticket31770 {

    private final static String TOPIC = "M000043.dbo.AGENCES_AGENTS";

    public static void main(String[] args) throws IOException {
        new Ticket31770().run();

    }

    private void run() throws IOException {
        Properties config = loadConfig();
        try (KafkaConsumer<Key, Envelope> consumer = new KafkaConsumer<>(config)) {

            TopicPartition partition = new TopicPartition(TOPIC, 4);
            consumer.assign(singletonList(partition));
            consumer.seek(
                    partition,
                    23328
            );
            boolean notFound = true;
            while (notFound) {
                ConsumerRecords<Key, Envelope> records = consumer.poll(Duration.ofMillis(100));
                Optional<ConsumerRecord<Key, Envelope>> maybeRecord =
                        StreamSupport.stream(records.spliterator(), false)
                                .filter(r -> "A0115".contentEquals(r.key().getAGNCID()) && 3398 == r.key().getAGNTID())
                                .findAny();

                maybeRecord
                        .map(ConsumerRecord::value)
                        .map(Envelope::getAfter)
                        .ifPresent(after -> {
                            System.out.println("after.getClass() = " + after.getClass());
                            System.out.println(after);
                        });

                notFound = ! maybeRecord.isPresent();

            }
        }
        
    }


    private Properties loadConfig() throws IOException {
        try (FileInputStream fis = new FileInputStream(getProperty("ccloud.config"))) {
            Properties properties = new Properties();
            properties.load(fis);
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, "confluent");
            properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
            return properties;
        }
    }
}
