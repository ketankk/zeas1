package com.itc.zeas.filereader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.itc.taphius.utility.ConfigurationReader;

public class FileCopyFromLocal {

	Logger logger= Logger.getLogger("FileCopyFromLocal");
	public SampleData copyFromLocal(String localPath,String HDFSPath) {
		
		SampleData file = new SampleData();
		
		logger.info("copyFromLocal method start , localPath:"+localPath+ " ,HDFSPath"+HDFSPath);
		System.out.println("localpath:"+localPath+"   :HDFSpath:"+HDFSPath);
		String destDir = HDFSPath.substring(0, HDFSPath.lastIndexOf("/")+1);
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS",ConfigurationReader.getProperty("NAMENODE_HOST"));
		try{
		FileSystem hdfs = FileSystem.get(conf);
		// Print the home directory
		Path localFilePath = new Path(localPath);
		//Path hdfsDirPath = new Path(destDir);
		long start = System.currentTimeMillis();
		/*if ( ! (hdfs.exists(hdfsDirPath))){
			hdfs.mkdirs(hdfsDirPath);
		    logger.info("create  HDFS directory if not exists.");
		}*/
		Path hdfsFilePath = new Path(HDFSPath);
        hdfs.copyFromLocalFile(localFilePath, hdfsFilePath);
		long end = System.currentTimeMillis();
		double diff = (double)(end-start)/1000;
		logger.info("successfully copy from local, time taken - "+diff+" seconds");
		long filelength = hdfs.getFileStatus(hdfsFilePath).getLen();
        System.out.println("lenth of file in bytes : "+filelength);
        // hdfs.delete(hdfsFilePath, false);
        //logger.info("Deleting test run sample file after successful ingestion");
        hdfs.close();
		System.out.println("sucess");
		file.setFileSize(filelength);		
		file.setTimeTaken(diff);
		logger.info("file infor :"+file.toString());
		}
		catch(Exception e) {
			logger.error("copy from local fails");
			System.out.println("fail:"+e.toString());
			return file;
		}
		// System.out.println("File copied from local to HDFS.");
		return file;
	}
	
	
}
