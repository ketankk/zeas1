package com.taphius.databridge.ingestion;

import org.apache.flume.Context;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.SpoolDirectorySource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CustomSource extends SpoolDirectorySource {

  private static final Logger logger = LoggerFactory.getLogger(CustomSource.class);  
 //instances read from db 

  @Override
  public synchronized void start() {   
    Context context = new Context();
//    context.put(SPOOL_DIRECTORY, tmpDir.getAbsolutePath());
    super.configure(context);
    MemoryChannel channel = new MemoryChannel();
    Configurables.configure(channel, context);
    logger.info("SpoolDirectorySource source starting with directory: {}");
    super.start();
  }
}
