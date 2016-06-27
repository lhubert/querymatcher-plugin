package com.hurence.logisland.querymatcher;

import com.hurence.logisland.event.Event;
import com.hurence.logisland.event.serializer.EventKryoSerializer;
import com.hurence.logisland.integration.testUtils.EmbeddedKafkaEnvironment;
import com.hurence.logisland.integration.testUtils.DocumentPublisher;
import com.hurence.logisland.integration.testUtils.RulesPublisher;
import kafka.admin.TopicCommand;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by lhubert on 20/04/16.
 */
public class QueryMatcherProcessorTest {

    static EmbeddedKafkaEnvironment context ;

    static String docset = "/documents/frenchpress";
    static String ruleset = "/rules/frenchpress";
    static String docstopic = "docs";
    static String rulestopic = "rules";
    static String matchestopic = "matches";

    static String[] arg1 = new String[] {"--topic", docstopic, "--partitions", "1", "--replication-factor", "1"};
    static String[] arg2 = new String[] {"--topic", rulestopic, "--partitions", "1","--replication-factor", "1" };
    static String[] arg3 = new String[] {"--topic", matchestopic, "--partitions", "1","--replication-factor", "1" };


    @BeforeClass
    public static void initEventsAndQueries() throws IOException {

        // create an embedded Kafka Context
        context = new EmbeddedKafkaEnvironment();

        // create docs input topic
        TopicCommand.createTopic(context.getZkClient(), new TopicCommand.TopicCommandOptions(arg1));
        TopicCommand.createTopic(context.getZkClient(), new TopicCommand.TopicCommandOptions(arg2));
        TopicCommand.createTopic(context.getZkClient(), new TopicCommand.TopicCommandOptions(arg3));

        // wait till all topics are created on all servers
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(context.getServers()), docstopic, 0, 5000);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(context.getServers()), rulestopic, 0, 5000);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(context.getServers()), matchestopic, 0, 5000);

        try {
            // send documents in path dir to topic
            DocumentPublisher publisher = new DocumentPublisher();
            publisher.publish(context, LuwakQueryMatcher.class.getResource(docset).getPath(), docstopic);

            // send the rules to rule topic
            RulesPublisher rpublisher = new RulesPublisher();
            rpublisher.publish(context, LuwakQueryMatcher.class.getResource(ruleset).getPath(), rulestopic);
        }

        catch (Exception e) {
            // log error
        }

    }
    @Test
    public void testProcess() throws Exception {


        // setup simple consumer for docs
        Properties consumerProperties = TestUtils.createConsumerProperties(context.getZkServer().connectString(), "group0", "consumer0", -1);
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));


        // deleting zookeeper information to make sure the consumer starts from the beginning
        // see https://stackoverflow.com/questions/14935755/how-to-get-data-from-old-offset-point-in-kafka
        context.getZkClient().delete("/consumers/group0");

        // reading rules
        RulesConsumer rconsumer = new RulesConsumer();
        List<MatchingRule> rules = rconsumer.consume(context, rulestopic);

        LuwakQueryMatcher matcher = new LuwakQueryMatcher(rules);
        QueryMatcherProcessor processor = new QueryMatcherProcessor(matcher);

        // starting consumer for docs...
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(docstopic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(docstopic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        if (iterator.hasNext()) {

            final EventKryoSerializer deserializer = new EventKryoSerializer(true);
            ByteArrayInputStream bais = new ByteArrayInputStream(iterator.next().message());
            Event deserializedEvent = deserializer.deserialize(bais);
            ArrayList<Event> list = new ArrayList<Event>();
            list.add(deserializedEvent);
            Collection<Event> result = processor.process(list);
            for (Event e : result) {
                System.out.println((String)e.get("name").getValue() + " : " + (String) e.get("matchingrules").getValue());
            }
            bais.close();

        } else {
            fail();
        }

        // cleanup
        consumer.shutdown();

    }


    @AfterClass
    public static void closeAll() throws IOException {
        for (KafkaServer server : context.getServers()) server.shutdown();
        context.getZkClient().close();
        context.getZkServer().shutdown();
    }
}