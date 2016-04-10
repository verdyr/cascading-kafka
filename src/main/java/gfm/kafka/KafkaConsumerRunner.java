package gfm.kafka;

import java.util.concurrent.Callable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import cascading.CascadingException;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerRunner implements Runnable, Callable<Throwable>
  {

  private static final Logger log = LoggerFactory.getLogger( KafkaConsumerRunner.class );

  private final String url;
  private final long jobId;

  private final ConsumerConnector consumer;
  private final String topic;
  private  ExecutorService executor;

  private KafkaStream m_stream;
//  private int m_threadNumber; 

  public KafkaConsumerRunner( String url, long jobId, String zk, String groupId, String a_topic )
    {
    this.url = url;
    this.jobId = jobId;

    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zk, groupId));
    this.topic = a_topic;
  }

  private static ConsumerConfig createConsumerConfig(String zk, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zk);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

  @Override
  public void run(int numThreads)
    {
    // Create kafka consumer from Kafka API
    // First create a Map that tells Kafka how many threads are provided for which topics
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(numThreads));
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
 
 
    // now launch all the threads
    //
    executor = Executors.newFixedThreadPool(numThreads);
 
    // now create an object to consume the messages
    //
    int threadNumber = 0;
    for (final KafkaStream stream : streams) {
        executor.submit(new KafkaConsumerRunner(stream, threadNumber));
        threadNumber++;
    }


    // Pass any error execptions upward
    if( executor.getError() != null )
      {
      log.info( "Exception info : " + executor.getError() );
      throw new CascadingException( executor.getError().toString() );
      }

    }

  @Override
  public Throwable call() throws Exception
    {
    try
      {
      this.run();
      }
    catch( Throwable throwable )
      {
      return throwable;
      }
    return null;
    }

  }
