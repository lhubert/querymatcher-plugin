package com.hurence.logisland.querymatcher;

import com.hurence.logisland.event.Event;
import com.hurence.logisland.event.serializer.EventKryoSerializer;
import com.hurence.logisland.integration.testUtils.EmbeddedKafkaEnvironment;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.TestUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by lhubert on 20/04/16.
 */
public class RulesConsumer {

    /**
     * This method will consume the events stored in the topic and transform them to
     * rules to be matched
     *
     * @param context
     * @param topic
     * @return a list of matchine rules
     */
    public List<MatchingRule> consume(EmbeddedKafkaEnvironment context, String topic) throws IOException {

        List<MatchingRule> rules = new ArrayList<MatchingRule>();

        // setup simple consumer for rules stored in the topic
        Properties consumerProperties = TestUtils.createConsumerProperties(context.getZkServer().connectString(), "group0", "consumer0", -1);
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));

        // deleting zookeeper information to make sure the consumer starts from the beginning
        // see https://stackoverflow.com/questions/14935755/how-to-get-data-from-old-offset-point-in-kafka
        context.getZkClient().delete("/consumers/group0");

        // starting consumer
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        if (iterator.hasNext()) {
            //String msg = new String(iterator.next().message(), StandardCharsets.UTF_8);

            final EventKryoSerializer deserializer = new EventKryoSerializer(true);
            ByteArrayInputStream bais = new ByteArrayInputStream(iterator.next().message());
            Event deserializedEvent = deserializer.deserialize(bais);
            MatchingRule rule = new MatchingRule((String) deserializedEvent.get("name").getValue(), (String) deserializedEvent.get("rule").getValue());
            rules.add(rule);
            System.out.println(deserializedEvent.toString());
            bais.close();

        }

        // cleanup
        consumer.shutdown();

        return rules;
    }
}
