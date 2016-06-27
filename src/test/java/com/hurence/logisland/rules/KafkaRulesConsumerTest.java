package com.hurence.logisland.rules;

import com.hurence.logisland.integration.testutils.EmbeddedKafkaEnvironment;
import com.hurence.logisland.querymatcher.MatchingRule;
import kafka.admin.TopicCommand;
import kafka.utils.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Properties;

import com.hurence.logisland.event.Event;
import com.hurence.logisland.event.serializer.EventKryoSerializer;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;


import static org.junit.Assert.*;

/**
 * Created by lhubert on 27/06/16.
 */
public class KafkaRulesConsumerTest {

    static EmbeddedKafkaEnvironment context ;

    public static String[] topic = new String[] {"--topic", "rules", "--partitions", "1","--replication-factor", "1" };

    @BeforeClass
    public static void initEventsAndQueries() throws IOException {

        // create an embedded Kafka Context
        context = new EmbeddedKafkaEnvironment();

        // create create rules topic
        TopicCommand.createTopic(context.getZkClient(), new TopicCommand.TopicCommandOptions(topic));

        // wait till all topics are created on all servers
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(context.getServers()), "rules", 0, 5000);

        // setup producer
        Properties properties = TestUtils.getProducerConfig("localhost:" + context.getPort());
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer producer = new Producer(producerConfig);

        final EventKryoSerializer kryoSerializer = new EventKryoSerializer(true);

        Event event = new Event("Rule");
        event.put("name", "String", "LukeSkyWalker");
        event.put("query", "String", "name:luke");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        kryoSerializer.serialize(baos, event);
        KeyedMessage<String, byte[]> data = new KeyedMessage("rules", baos.toByteArray());
        baos.close();

        List<KeyedMessage> messages = new ArrayList<>();
        messages.add(data);

        producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
        producer.close();

    }

    @Test
    public void testConsume() throws Exception {
        // reading rules
        KafkaRulesConsumer rconsumer = new KafkaRulesConsumer();
        List<MatchingRule> rules = rconsumer.consume(context, "rules", "rules", "consumer0");

        for (MatchingRule rule: rules) {
            System.out.println(rule.getName() + " " + rule.getQuery());
        }

        context.close();
    }
}