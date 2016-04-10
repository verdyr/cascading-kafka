package gfm.kafka;

import cascading.CascadingException;
import cascading.tap.Tap;
import org.apache.hadoop.conf.Configuration;
import riffle.process.DependencyIncoming;
import riffle.process.DependencyOutgoing;
import riffle.process.ProcessComplete;
import riffle.process.ProcessConfiguration;
import riffle.process.ProcessCounters;
import riffle.process.ProcessStart;
import riffle.process.ProcessStop;

import java.beans.ConstructorProperties;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@riffle.process.Process
public class KafkaRiffle
  {

  /** List of source taps */
  private final Collection<Tap> sources;

  /** sink of the Riffle. */
  private final Tap sink;

  private Future<Throwable> future;

  // kafka endpoint url
  private String url;
  // kafka id to connect to
  private long jobId;

  private String zk;
  private String groupId;
  private String topic;

  @ConstructorProperties({"sources", "sink", "url", "jobId", "zk", "groupId", "topic"})
  public KafkaRiffle( Collection<Tap> sources, Tap sink, String url, long jobId, String zk, String groupId, String topic )
    {
    this.sources = sources;
    this.sink = sink;
    this.url = url;
    this.jobId = jobId;
    this.zk = zk; 
    this.groupId = groupId; 
    this.topic = topic;;
    }

  @ProcessCounters
  public Map<String, Map<String, Long>> processCounters()
    {
    Map<String, Map<String, Long>> counters = new HashMap<String, Map<String, Long>>();
    return counters;
    }

  @ProcessStart
  public void start()
    {
    internalStart();
    }

  /**
   * Custom part, calling external Kafka Consumer process, direct API
   */
  private synchronized void internalStart()
    {
    if( future != null )
      return;

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Callable<Throwable> kafkaConsumerRunner = new KafkaConsumerRunner( this.url, this.jobId, this.zk, this.groupId, this.topic );
    future = executorService.submit( kafkaConsumerRunner );
    executorService.shutdown();

    }

  @ProcessStop
  public void stop()
    {
    }

  @ProcessComplete
  public void complete()
    {

    internalStart();

    try
      {
      Throwable throwable = future.get();
      if( throwable != null )
        {
        if( throwable instanceof RuntimeException )
          throw ( (RuntimeException) throwable );
        else
          throw new CascadingException( "exception when future.get() on Kafka Consumer", throwable );
        }

      }
    catch( Exception exception )
      {
      throw new CascadingException( "exception while executing Kafka Consumer", exception );
      }
    }

  @ProcessConfiguration
  public Configuration getConfiguration()
    {
    return new Configuration(  );
    }

  @DependencyOutgoing
  public Collection outGoing()
    {
    return Collections.unmodifiableCollection( Arrays.asList( sink ) );
    }

  @DependencyIncoming
  public Collection incoming()
    {
    return sources;
    }
  }
