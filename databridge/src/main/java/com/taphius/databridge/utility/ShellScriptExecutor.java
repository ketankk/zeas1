package com.taphius.databridge.utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;



public class ShellScriptExecutor {
    public static Logger LOG = Logger.getLogger(ShellScriptExecutor.class);
    public static final String BASH = "/bin/bash";
   
    public static int runScript(String...args){
        
        
     LOG.info("Going to execute shell script - "+args[1]);   
     int status=1;

      ProcessBuilder  pb = new ProcessBuilder(args);
   // Redirect the errorstream
      pb.redirectErrorStream(true);
      pb.redirectErrorStream(true);
      Process p;
    try {
        p = pb.start();
   
      BufferedReader br = new BufferedReader(new InputStreamReader(
              p.getInputStream()));
      System.out.println(p.waitFor());
      System.out.println("br output =="+br.toString() + "=="+br.readLine());
      LOG.info("Process Builder started..");
      while (br.ready()) {
          String str=br.readLine().trim();
          System.out.println("str=="+str);
          LOG.info("Console Output "+str);
      } 
      status=0;
    } catch (IOException | InterruptedException e) {
        // TODO Auto-generated catch block
    	LOG.error("Excepion while running Script file "+e.getMessage());
        e.printStackTrace();
    } 
    
     return status;
    }

}
