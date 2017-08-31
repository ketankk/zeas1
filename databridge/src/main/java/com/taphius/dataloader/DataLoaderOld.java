package com.taphius.dataloader;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Little example which copies a locally mounted file to hdfs. */
public class DataLoaderOld {

  private static Logger logger = Logger.getLogger(DataLoaderOld.class.getName());
  private static final String DEFAULT_HOST = "hdfs://hadooplab.bigdataleap.com:8020/";

  public void init(String[] files, String srcDir) {
    Configuration conf = new Configuration();
//    conf.addResource(new Path(System.getenv("HADOOP_INSTALL") + "/etc/hadoop/core-site.xml"));
    conf.addResource("/home/hadoop/lab/software/hadoop-2.3.0/etc/hadoop/core-site.xml");
    List<String> list = new ArrayList<String>();
    for (String str : files) {
      list.add(srcDir+"/"+str);
    }
    try {
      copyFiles(list, conf);
    } catch (IOException e) {
      logger.log(Level.SEVERE, e.toString());
    } catch (URISyntaxException e) {
      logger.log(Level.SEVERE, e.toString());
    }
  }

  private void copyFiles(List<String> listOfFiles, Configuration conf)
      throws IOException, URISyntaxException {
    FileSystem fs = FileSystem.get(new URI(DEFAULT_HOST), conf);
    for (String s : listOfFiles) {
      Path fromLocal = new Path(s);
      Path toHdfs = new Path(DEFAULT_HOST + s);
      fs.copyFromLocalFile(fromLocal, toHdfs);
    }
  }

  public static void main(String[] args) {
    DataLoaderOld loader = new DataLoaderOld();
//    loader.init(args);
  }

}
