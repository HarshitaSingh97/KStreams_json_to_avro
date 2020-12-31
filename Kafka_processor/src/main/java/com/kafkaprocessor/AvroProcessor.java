package com.kafkaprocessor;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.examples.pageview.JsonPOJODeserializer;
import org.apache.kafka.streams.examples.pageview.JsonPOJOSerializer;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.simple.JSONObject;

@Configuration
public class AvroProcessor {

    public static final String INPUT_TOPIC = "intopic";
    public static final String OUTPUT_TOPIC = "outtopic";

    private static final Logger logger = LoggerFactory.getLogger(AvroProcessor.class);
    static Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-avroconverter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());
        props.put("schema.registry.url", "http://127.0.0.1:8081");

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    static json_to_avro avro_converter(JSONObject json_record){
//        System.out.print(json_record.get("name"));
        json_to_avro av_record = json_to_avro.newBuilder()
                .setName((String) json_record.get("name")).setTryNo((Integer) json_record.get("try")).build();
        return av_record;
    }
    static void createAvroStream(final StreamsBuilder builder) {
        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<JSONObject> jsonSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", JSONObject.class);
        jsonSerializer.configure(serdeProps, false);

        final Deserializer<JSONObject> jsonDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", JSONObject.class);
        jsonDeserializer.configure(serdeProps, false);
        final Serde<JSONObject> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                "http://localhost:8081");
        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(serdeConfig, true); // `false` for record values


        final KStream<String, JSONObject> source = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), jsonSerde));

        final KStream<String, json_to_avro> avro_stream = source
                .mapValues(value -> avro_converter(value));

        avro_stream.to(OUTPUT_TOPIC);
    }

    public static void main(final String[] args) {
        final Properties props = getStreamsConfig();

        final StreamsBuilder builder = new StreamsBuilder();
        createAvroStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-avroconverter-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
