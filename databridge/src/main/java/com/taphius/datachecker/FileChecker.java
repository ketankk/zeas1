package com.taphius.datachecker;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.utility.utility.ConfigurationReader;
import org.apache.log4j.Logger;

import com.taphius.databridge.dao.EntityDefinationDAO;
import com.taphius.databridge.model.DataSourcerAttributes;
import com.taphius.databridge.scheduler.RegisterSchedulerUtility;
import com.taphius.databridge.utility.ShellScriptExecutor;
import com.taphius.dataloader.DataLoader;
import com.taphius.pipeline.notification.ReadNotification;
import com.zdp.dao.ZDPDataAccessObjectImpl;
import com.itc.zeas.ingestion.model.ZDPScheduler;

public class FileChecker
{
  static final Logger LOG = Logger.getLogger(FileChecker.class);
  private WatchService myWatcher;
  private EntityDefinationDAO entityDAO;
  private HashMap<String, String> ingestionMapping;
  MyWatchQueueReader fileWatcher;
  
  public EntityDefinationDAO getEntityDAO()
  {
    return this.entityDAO;
  }
  
  public void setEntityDAO(EntityDefinationDAO entityDAO)
  {
    this.entityDAO = entityDAO;
  }
  
  public WatchService getMyWatcher()
  {
    return this.myWatcher;
  }
  
  public void setMyWatcher(WatchService myWatcher)
  {
    this.myWatcher = myWatcher;
  }
  
  public void init()
  {
    try
    {
      LOG.info("Starting FileCheker watcher service...");
      this.myWatcher = FileSystems.getDefault().newWatchService();
      this.fileWatcher = new MyWatchQueueReader(this.myWatcher);
      this.ingestionMapping = new HashMap();
      Thread th = new Thread(this.fileWatcher, "FileWatcher");
      th.start();
      
      List<DataSourcerAttributes> totalEntries = this.entityDAO.getTotalIngestions("DataIngestion");
      System.out.println("total Entries : " + totalEntries);
      RegisterSchedulerUtility schedulerUtility = new RegisterSchedulerUtility();
      schedulerUtility.setDataChecker(this);
      schedulerUtility.registerScheduler(totalEntries);
    }
    catch (Exception e)
    {
      LOG.error("There is an error iniating File checker - " + e.getMessage());
    }
  }
  
  public Map<String, String> getIngestionMapping()
  {
    return this.ingestionMapping;
  }
  
  public void setIngestionMapping(HashMap<String, String> ingestionMapping)
  {
    this.ingestionMapping = ingestionMapping;
  }
  
  class MyWatchQueueReader
    implements Runnable
  {
    private WatchService myWatcher;
    
    public MyWatchQueueReader(WatchService myWatcher)
    {
      this.myWatcher = myWatcher;
    }
    
    public void run()
    {
      for (;;)
      {
        WatchKey key;
        try
        {
          key = this.myWatcher.take();
        }
        catch (InterruptedException ex)
        {
          return;
        }
        for (WatchEvent<?> event : key.pollEvents())
        {
          WatchEvent.Kind<?> kind = event.kind();
          
          WatchEvent<Path> ev = (WatchEvent<Path>) event;
          Path fileName = (Path)ev.context();
          
          Path fullPath = ((Path)key.watchable()).resolve(fileName).getParent();
          
         // FileChecker.LOGGER.debug("File name - " + fileName.toString());
          //FileChecker.LOGGER.debug("list of files in directory : " + getFileCount(fullPath.toFile()));
         // FileChecker.LOGGER.debug("fullPath with directory : " + fullPath);
          if (kind != StandardWatchEventKinds.OVERFLOW) {
            if (kind == StandardWatchEventKinds.ENTRY_CREATE)
            {
              FileChecker.LOG.debug("Got file/directory created event for - " + fileName.toString());
              FileChecker.LOG.info("Got file/directory created event for - " + fileName.toString());
              ZDPDataAccessObjectImpl imp = new ZDPDataAccessObjectImpl();
              
              String data1 = (String)FileChecker.this.ingestionMapping.get(fullPath.toFile().toString());
              
              String[] cols1 = data1.split(",");
      
              for (int i = 0; i < cols1.length; i++) {
                System.out.println("value at col :" + i + ":" + cols1[i]);
              }
              String projectId = imp.getSchedulerProjectId(cols1[3]);
              
              int fileCount = getFileCount(fullPath.toFile());
              boolean doneFile = getDoneFile(fullPath.toFile());
              FileChecker.LOG.debug("projectId : " + projectId);
              if ((fileCount > 0) && (!projectId.equals("")))
              {
                ZDPScheduler scheduler = imp.getScheduler(Long.valueOf(projectId).longValue());
                String type = scheduler.getType();
                String status = scheduler.getStatus();
                
                FileChecker.LOG.debug("event Type : " + type);
                FileChecker.LOG.debug("scheduler status : " + status);
                if ((type.equalsIgnoreCase("event_based")) && (status.equalsIgnoreCase("Active")))
                {
                  FileChecker.LOG.debug("type : " + type);
                  FileChecker.LOG.debug("status : " + status);
                  FileChecker.LOG.debug("dataset : " + cols1[4]);
                  if (!doneFile)
                  {
                    String[] strArry = new String[3];
                    String SQOOP_SCRIPT_PATH = System.getProperty("user.home") + "/zeas/Config/filecreate.sh";
                    
                    String SHELL_SCRIPT_TYPE = "/bin/bash";
                    strArry[0] = SHELL_SCRIPT_TYPE;
                    strArry[1] = SQOOP_SCRIPT_PATH;
                    strArry[2] = fullPath.toString();
                    
                    ShellScriptExecutor shExe = new ShellScriptExecutor();
                    
                    shExe.runScript(strArry);
                  }
                  try
                  {
                    TimeUnit.SECONDS.sleep(30L);
                  }
                  catch (InterruptedException e)
                  {
                    FileChecker.LOG.debug("sleep over");
                    e.printStackTrace();
                  }
                }
              }
              data1 = null;
              imp = null;
        
              if (fileName.toString().startsWith("_DONE"))
              {
                String data = (String)FileChecker.this.ingestionMapping.get(fullPath.toFile().toString());
                String[] cols = data.split(",");
                
                long sIngestionTime = 0L;
                
                sIngestionTime = System.currentTimeMillis();
                
                Properties props = new Properties();
                props.setProperty("ingestion-id", cols[0]);
                props.setProperty("source-dir", cols[1]);
                props.setProperty("schedularName", cols[2]);
                props.setProperty("schemaName", cols[3]);
                props.setProperty("datasetName", cols[4]);
                props.setProperty("targetHDFSPath", cols[5]);
                props.setProperty("batchFrequency", cols[6]);
                props.setProperty("fileFormat", cols[7]);
                props.setProperty("fileType", cols[8]);
                props.setProperty("scheduleTime", String.valueOf(sIngestionTime));
                  props.setProperty("namenode", ConfigurationReader.getProperty("HDFS_FQDN"));


                String IS_FIRST_RECORD_HEADER = cols[9];
                FileChecker.LOG.debug("IS_FIRST_RECORD_HEADER: " + IS_FIRST_RECORD_HEADER);
                props.setProperty("first.record.header", IS_FIRST_RECORD_HEADER);
                DataLoader loader = new DataLoader();
                try {
                  loader.run(props, fullPath.toFile().toString());
                } catch (ZeasException e) {
                  e.printStackTrace();
                }
              }
              else
              {
                File f = ((Path)key.watchable()).resolve(fileName).toFile();
                if ((f.isDirectory()) && (!f.getName().equalsIgnoreCase("archive"))) {
                  try
                  {
                    Path fPath = ((Path)key.watchable()).resolve(fileName);
                    registerRecursive(fPath, this.myWatcher);
                    FileChecker.this.ingestionMapping.put(f.toString(), FileChecker.this.ingestionMapping.get(fullPath.toFile().toString()));
                    
                    FileChecker.LOG.debug("registering sub folder to watcher - " + f.toString());
                  }
                  catch (IOException e)
                  {
                    FileChecker.LOG.error(e.toString());
                  }
                }
              }
            }
            else if (kind != StandardWatchEventKinds.ENTRY_DELETE)
            {
              if (kind == StandardWatchEventKinds.ENTRY_MODIFY)
              {
                FileChecker.LOG.debug("Got file Modify event.");
                if (fileName.toString().startsWith("notify"))
                {
                  FileChecker.LOG.info("Recieved Modify event for - " + fileName.toString());
                  ReadNotification notifier = new ReadNotification();
                  try
                  {
                    notifier.checkNotification();
                  }
                  catch (SQLException e)
                  {
                    e.printStackTrace();
                  }
                }
              }
            }
          }
        }
        boolean valid = key.reset();
        if (!valid) {
          break;
        }
      }
    }
    
    private boolean getDoneFile(File file)
    {
      File[] files = file.listFiles();
      File[] arr$ = files;int len$ = arr$.length;int i$ = 0;
      if (i$ < len$)
      {
        File f1 = arr$[i$];
        FileChecker.LOG.info(" Is _DONE File : " + f1.getName());
        FileChecker.LOG.info(" Is _DONE File Size : " + f1.getName().length());
        if (f1.getName().equalsIgnoreCase("_DONE")) {
          FileChecker.LOG.info(" Return type true");
        }
        return true;
      }
      return false;
    }
    
    private int getFileCount(File file)
    {
      File[] files = file.listFiles();
      int count = 0;
      for (File f1 : files)
      {
       // FileChecker.LOGGER.info(" Is _DONE File : " + f1.getName());
        if ((!f1.isDirectory()) && (!f1.getName().equalsIgnoreCase("_DONE"))) {
          count++;
        }
      }
      return count;
    }
    
    private String getFiles(File f, String fname)
    {
      String result = "";
      
      File[] paths = f.listFiles();
      for (File path : paths) {
        if ((!path.isFile()) || (!path.toString().contains(fname))) {
          result = result + path.getName() + ",";
        }
      }
      FileChecker.LOG.info("Found files at path -" + f.getPath() + "-----" + result);
      return result.substring(0, result.length() - 1);
    }
    
    private void registerRecursive(Path root, final WatchService myWatcher)
      throws IOException
    {
      Files.walkFileTree(root, new SimpleFileVisitor()
      {
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
          throws IOException
        {
          dir.register(myWatcher, new WatchEvent.Kind[] { StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY });
          return FileVisitResult.CONTINUE;
        }
      });
    }
  }
}
