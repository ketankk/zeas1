package com.itc.zeas.machineLearning;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class MachineLearningExecutor {

    /**
     * @param args
     */
    public static void main(String[] args) {

        MachineLearningExecutor obj= new MachineLearningExecutor();
        System.out.println((int)(Double.parseDouble("0.6543")*100));
        //        String accuracy=obj.getResult(args[0], "/root/ZEAS/data/2007/delayFromHoliday", "/root/ZEAS/data/2008/HiveQuery2008");
        //        System.out.println("accuracy:-"+accuracy);
    }

    // model means  type of algorithm, if user provide "regression" values the execute regression algorithm otherwise 
    //by  default random forest algo execute
    //@2nd parameter is training data
    //@3rd p[rameter is testing data   
    public int execute(String algorithm,String trainData,String testData) {
        System.out.println("Find inputs here ==="+algorithm+"--"+trainData+"--"+testData);
        String result="";
        boolean isWrite=false;
        String script="/root/machine_script/py_randomforest.py";
        if(algorithm.equalsIgnoreCase("Logistic Regression")) {
            script="/root/machine_script/py_regression.py";
            System.out.println("Running Logistic Regression..");
        }else {
            script="/root/machine_script/py_randomforest.py";
        }
        
        
        BufferedReader br =null;
        try {

            Runtime r = Runtime.getRuntime();
          ProcessBuilder pb =  new ProcessBuilder("python",script, trainData, testData  );
          pb.directory(new File("/usr/bin"));
//            Process p = r.exec("python "+script+" "+trainData+" "+testData);
          Process p = pb.start(); 
             br = new BufferedReader(new InputStreamReader(
                    p.getInputStream()));
            System.out.println(p.waitFor());
            System.out.println("br output =="+br.toString() + "=="+br.readLine());
            while (br.ready()) {
                String str=br.readLine().trim();
                System.out.println("str=="+str);
                if(isWrite) {
                    result=str;
                }
                if(str.equalsIgnoreCase("accuracy")) {
                    isWrite=true;
                }
            }
            

        } catch (Exception e) {
            System.out.println( e.getMessage());        
        }
        finally{
        	//closing bufferedReader connections if it is not null.
            if(br != null){
            	try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
        }
        System.out.println("output *****************************");
        System.out.println(result);
        return (int)(Double.parseDouble(result)*100);
    }
    

}
