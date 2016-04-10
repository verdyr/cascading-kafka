package gfm.kafka;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.flow.process.ProcessFlow;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class Main
  {
  private static final Logger log = LoggerFactory.getLogger( Main.class );

  public static void main( String[] args )
    {

    // Set some App properties
    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, Main.class );
    AppProps.addApplicationTag( properties, "integration" );
    AppProps.addApplicationTag( properties, "GFM:development" );
    AppProps.setApplicationName( properties, "Custom Flow - Kafka" );

    // Use hadoop flow connector
    Hadoop2MR1FlowConnector flowConnector = new Hadoop2MR1FlowConnector( properties );

    // To align with Streamer code properties:

    String inPath = args[ 0 ]; // <Input file>
    String outPath = args[ 1 ]; // <Output file>
    String kafkaOutPath = args[ 2 ]; // <Kafka Source Topics path>
    String url = args[ 3 ]; // "http://ap-hdpen101t.oneadr.net:kafkaPort"
    long jobId = Long.parseLong( args[ 4 ] ); // <Kafka Topic ID>


    // create the source tap for <Input file>
    Tap inTap = new Hfs( new TextDelimited( true, "\t" ), inPath );

    // create the sink tap <Output file>
    Tap outTap = new Hfs( new TextDelimited( true, "\t" ), outPath );

    // get Kafka Tap <Kafka Source Topics>
    Tap kafkaOutTap = new Hfs( new TextDelimited( true, "\t" ), kafkaOutPath );

    // specify a pipe to connect the taps, as simple copy
    Pipe copyPipe = new Pipe( "copy" );

    // connect the taps, pipes, etc., into a flow
    FlowDef flowDef = FlowDef.flowDef()
      .addSource( copyPipe, inTap )
      .addTailSink( copyPipe, outTap )
      .setName( "Data Copy test" );

    // Create a process flow to contain Kafka consumer
    ProcessFlow kafkaFlow = new ProcessFlow( "Run Kafka Consumer",
      new KafkaRiffle( Collections.unmodifiableCollection( Arrays.asList( outTap ) ),
        kafkaOutTap,
        url,
        jobId ) );

    // Now connecting regular flow, dataPrepFlow that copies HDFS file
    Flow dataPrepFlow = flowConnector.connect( flowDef );

    // Connect kafkaFlow and dataPrepFlow using a Cascade
    CascadeConnector connector = new CascadeConnector();
    Cascade cascade = connector.connect( kafkaFlow, dataPrepFlow );

    // Run the Cascade
    cascade.start();
    cascade.complete();

    }

  }

