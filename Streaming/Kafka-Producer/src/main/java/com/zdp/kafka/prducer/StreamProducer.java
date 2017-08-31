package com.zdp.kafka.prducer;
/**
 * Created by kristy.patel on 4/30/2015.
 */
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.Time;

public class StreamProducer {

	public static final String BASH = "/bin/bash";
    public static void main(String[] args)  {
        Properties props = new Properties();
        props.put("metadata.broker.list", "ec2-54-174-149-226.compute-1.amazonaws.com:6667");
        //props.put("bootstrap.servers", "ec2-54-174-149-226.compute-1.amazonaws.com:6667");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        //props.put("value.serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type","async");
        props.put("request.required.acks","1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        
        Date today = new Date();
		String date = (new SimpleDateFormat("dd-MM-yyyy hh:mm:00").format(today));
		System.out.println(date+"\n--------------------------");
        
        Connection con;
        ArrayList<Integer> customerList=null;
        ArrayList<Integer> puckList=null;
        try {
			con=connectToDB();
			customerList=getCustomerList(con);
			puckList=getPuckList(con);
		} catch (SQLException e) {
			e.printStackTrace();
		}
     
        for (int i=0;i<10	;i++){
        	producer.send(generateMessage("test", customerList, puckList));
        }
        producer.close();
    }
    
    
  
    	public static Connection connectToDB() throws SQLException {
    		//String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
    		String driverName = "org.apache.hive.jdbc.HiveDriver";
    		try {
    			Class.forName(driverName);
    		} catch (ClassNotFoundException e){
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    			System.exit(1);
    		}
    		System.out.println("establishing connection.");
    		Connection con = DriverManager.getConnection("jdbc:hive2://ec2-54-174-149-226.compute-1.amazonaws.com:10000/default;auth=noSasl", "hive", "hive123");
    		//Connection con = DriverManager.getConnection("jdbc:hive://localhost:10000/iotzeas", "", "");
    		System.out.println("established connection.");
    		return con;
//    		Statement stmt = con.createStatement();
//    		stmt.executeUpdate("use iotzeas");
//    		ResultSet rs=stmt.executeQuery("select * from customerdata");
//    		while (rs.next()) {              
//    		    System.out.println(rs.getString(1)+":"+rs.getString(2)+":"+rs.getString(3)+":"+rs.getString(4)+":"+rs.getString(5)+":"+rs.getString(6)+":"+rs.getString(7));
//    		}

    	}
    	
    	public static ArrayList<Integer> getCustomerList(Connection con) throws SQLException{
    		ArrayList<Integer> customerList = new ArrayList<Integer>();
    		Statement stmt = con.createStatement();
    		stmt.executeUpdate("use iotzeas");
    		ResultSet rs=stmt.executeQuery("select * from customerdata");
    		while (rs.next()) {    
    			customerList.add(rs.getInt(1));
    		}
    		return customerList;
    	}
    	
    	public static ArrayList<Integer> getPuckList(Connection con) throws SQLException{
    		ArrayList<Integer> puckList = new ArrayList<Integer>();
    		Statement stmt = con.createStatement();
    		stmt.executeUpdate("use iotzeas");
    		ResultSet rs=stmt.executeQuery("select * from pucks");
    		while (rs.next()) {    
    			puckList.add(rs.getInt(1));
    		}
    		return puckList;
    	}
    	
    	public static KeyedMessage<String, String> generateMessage(String topic, ArrayList<Integer> customerList, ArrayList<Integer> puckList){
    		int customerId = customerList.get(randInt(0, customerList.size()-1));
    		int puckId = puckList.get(randInt(0, puckList.size()-1));
    		int aggregateTime = randInt(1, 30);
    		int lastSeen = randInt(0, 24*60*60);
    		Calendar cal = Calendar.getInstance();
    		cal.set(Calendar.HOUR_OF_DAY,lastSeen/(60*60));
    		cal.set(Calendar.MINUTE,(lastSeen%(60*60))/60);
    		cal.set(Calendar.SECOND,(lastSeen%(60*60))%60);
    		cal.set(Calendar.MILLISECOND,0);
    		Date d = cal.getTime();
    		System.out.println("Sending "+ topic+":::"+ customerId+","+puckId+","+aggregateTime+","+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(d));
    		return new KeyedMessage<String, String>(topic, customerId+","+puckId+","+aggregateTime+","+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(d));	
    	}
    	
    	public static int randInt(int min, int max) {
    	    Random rand = new Random();
    	    int randomNum = rand.nextInt((max - min) + 1) + min;
    	    return randomNum;
    	}
    	
    	public static int runScript(String...args){
    		//LOG.info("Going to execute shell script - "+args[1]);   


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
    			while (br.ready()) {
    				String str=br.readLine().trim();
    				System.out.println("str=="+str);
    			}   
    		} catch (IOException ioe) {
    			// TODO Auto-generated catch block
    			ioe.printStackTrace();
    		} 
    		catch (InterruptedException ie) {
    			// TODO Auto-generated catch block
    			ie.printStackTrace();
    		} 

    		return 0;
    	}

}
