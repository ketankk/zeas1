package com.taphius.databridge.ingestion;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.Charsets;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.sink.hdfs.HDFSEventSink;
import org.apache.flume.source.SpoolDirectorySource;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants;

import com.google.common.io.Files;

public class IngestionDefination {
	
	private static SpoolDirectorySource source;
	 private static HDFSEventSink hdfs;
	private static MemoryChannel channel;
	private static File tmpDir;
	
 public static void main(String[] args) throws InterruptedException, IOException, EventDeliveryException {
	 source = new SpoolDirectorySource();
	 hdfs = new HDFSEventSink();
	    channel = new MemoryChannel();
	    Configurables.configure(channel, new Context());
	    List<Channel> channels = new ArrayList<Channel>();
	    channels.add(channel);
	    ChannelSelector rcs = new ReplicatingChannelSelector();
	    rcs.setChannels(channels);
	    source.setChannelProcessor(new ChannelProcessor(rcs));
	    tmpDir = new File("/home/hadoop/lab/data/mano");
	
	    
	    Context context = new Context();
	    File f1 = new File("/home/hadoop/lab/data/mano/file1");
	    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
	                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
	                f1, Charsets.UTF_8);
	    context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
	        tmpDir.getAbsolutePath());
	    context.put(SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER,
	        "true");
	    context.put(SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER_KEY,
	        "fileHeaderKeyTest");
	    Configurables.configure(source, context);
	    source.start();
	    Thread.sleep(500);
	    
	    
	    Context context1 = new Context();
	   
	       // context.put("hdfs.path", testPath + "/%Y-%m-%d/%H");
	        context1.put("hdfs.path", "hdfs://hadooplab.bigdataleap.com/lab/copied");
	      context1.put("hdfs.filePrefix", "mano");
	       //  context1.put("hdfs.rollCount", String.valueOf(20));
	       //  context1.put("hdfs.rollInterval", "0");
	       //  context1.put("hdfs.rollSize", "0");
	      //   context1.put("hdfs.batchSize", String.valueOf(20));
	      context1.put("hdfs.writeFormat", "Text");
	      //  context.put("hdfs.useRawLocalFileSystem",
	      //       Boolean.toString(useRawLocalFileSystem));
	        context1.put("hdfs.fileType", "DataStream");
	    
	         Configurables.configure(hdfs, context1);
	     
	       /* Channel channel = new MemoryChannel();
	         Configurables.configure(channel, context);*/
	   
	        hdfs.setChannel(channel);
	        hdfs.start();
	        hdfs.process();
	  /*  Transaction txn = channel.getTransaction();
	    txn.begin();	   
	    Event e = channel.take();
	    System.out.println("manohar =="+e.getBody().toString());*/
	   /* Assert.assertNotNull("Event must not be null", e);
	    Assert.assertNotNull("Event headers must not be null", e.getHeaders());
	    Assert.assertNotNull(e.getHeaders().get("fileHeaderKeyTest"));
	    Assert.assertEquals(f1.getAbsolutePath(),
	        e.getHeaders().get("fileHeaderKeyTest"));*/
	    //txn.commit();
	//    txn.close();
	
}
	
	

}
