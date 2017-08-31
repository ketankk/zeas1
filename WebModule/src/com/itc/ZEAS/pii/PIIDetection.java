package com.itc.zeas.pii;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;


/**
 * 
 * PIIDetection class is written to identify the Personal Information Identifier (PII)
 * for each column. It accepts the column header and column data in map and list respectively
 * and output a map with column name and its PII values(Y/N). 
 * 
 * @author 16258,18319  
 * Date: 21/04/2015
 */

public class PIIDetection implements Callable<Map<String, String>> {

	Pattern phonePattern;
	Pattern bankPattern;
	Pattern emailPattern;
	Pattern ipv4v6Pattern;
	Pattern pancardPattern;
	Pattern SSNPattern;
	Pattern creditCardPattern;

	Logger logger = Logger.getLogger("PIIDetection");
	

	List<String> column;
	String colName;
	int flag = 0;

	public PIIDetection() {
	}

	public PIIDetection(List<String> column, String colName) {
		this.column = column;
		this.colName = colName;
		initPattern();
	}

	private void initPattern() {
		
		phonePattern 	  = Pattern.compile(RegExConstant.phoneregex);
		bankPattern 	  = Pattern.compile(RegExConstant.bankRegex);
		emailPattern 	  = Pattern.compile(RegExConstant.emailRegex);
		ipv4v6Pattern 	  = Pattern.compile(RegExConstant.ipv4v4v6);
		pancardPattern 	  = Pattern.compile(RegExConstant.pancardRegex);
		SSNPattern 		  = Pattern.compile(RegExConstant.SSNRegex);
		creditCardPattern = Pattern.compile(RegExConstant.creditCardRegex);
	}

	
	/**
	 * This method performs the pattern matching of phone no
	 * 
	 * @param: Accepts phone no as string for pattern matching
	 * @return: True if the pattern is matched and false otherwise
	 */
	private boolean phoneMatch(String s) {

		Matcher phoneMatcher = phonePattern.matcher(s);
		if (phoneMatcher.matches()) {
			return true;
		} else
			return false;
	}
	
	/**
	 * This method performs the pattern matching of bank a/c no
	 * 
	 * @param Accepts bank a/c no as string for pattern matching
	 * @return True if the pattern is matched and false otherwise
	 */
	private boolean bankMatch(String s) {

		Matcher bankMatcher = bankPattern.matcher(s);
		if (bankMatcher.find()) {
			return true;
		} else
			return false;

	}

	/**
	 * This method performs the pattern matching of email
	 * 
	 * @param: Accepts email as string for pattern matching
	 * @return: True if the pattern is matched and false otherwise
	 */
	private boolean emailMatch(String s) {

		Matcher emailMatcher = emailPattern.matcher(s);
		if (emailMatcher.find()) {
			return true;
		} else {
			return false;
		}

	}

	
	/**
	 * This method performs the pattern matching of Ip address
	 * 
	 * @param: Accepts Ip as string for pattern matching
	 * @return: True if the pattern is matched and false otherwise
	 */
	private boolean ipv4v6Match(String s) {

		Matcher ipv4v6Matcher = ipv4v6Pattern.matcher(s);
		if (ipv4v6Matcher.find()) {
			return true;
		} else
			return false;
	}

	
	/**
	 * This method performs the pattern matching of pancard
	 * 
	 * @param: Accepts Pancard as string for pattern matching
	 * @return: True if the pattern is matched and false otherwise
	 */
	private boolean pancardMatch(String s) {

		Matcher pancardMatcher = pancardPattern.matcher(s);
		if (pancardMatcher.find()) {
			return true;
		} else
			return false;
	}

	
	/**
	 * This method performs the pattern matching of SSN
	 *
	 * @param: Accepts SSN as string for pattern matching
	 * @return: True if the pattern is matched and false otherwise
	 */
	private boolean SSNMatch(String s) {
		
		String tempSSN = s.replaceAll("-", "").replace(" ", ""); //Replacing all "-" and space with blank.
		Matcher SSNMatcher = SSNPattern.matcher(tempSSN);
		if (SSNMatcher.find()) {
			return true;
		} else
			return false;
	}

	
	/**
	 * This method performs the pattern matching of credit card
	 * 
	 * @param: Accepts Credit Card no as string for pattern matching
	 * @return: True if the pattern is matched and false otherwise
	 */
	private boolean creditCardMatch(String s) {

		String tempCC = s.replaceAll("\\D+","").replaceAll("-", "").replace(" ", ""); //Replacing all "-" ,digits, and space with blank. 

		Matcher creditCardMatcher = creditCardPattern.matcher(tempCC);
		if (creditCardMatcher.find()) {
			if (validate(tempCC)) {
				return true;
			}
			else {
				return false;
				}
		} else
			return false;
	}
	
	

	/**
	 * This method performs the pattern matching of credit card using alogrithm
	 * 
	 * @param: Accepts Credit Card no as string for pattern matching
	 * @return: True if the pattern is matched and false otherwise
	 */
	private static boolean validate(String str) {
        String reverse = new StringBuilder().append(str).reverse().toString();
     
        int[] array = new int[str.length()];
        for (int i = 0; i < array.length; i++) {
            array[i] = Integer.parseInt("" + reverse.charAt(i));
        }
        int sum = 0;
        for (int i = 0; i < array.length; i++) {
            if (i % 2 == 1) {
                array[i] *= 2;
                if (array[i] > 9) {
                    array[i] -= 9;
                }
            }
            sum += array[i];
        }
        return sum % 10 == 0;
      
    }


	
	/**
	 * This method performs the pattern matching for various field types
	 * 
	 * @param: Accepts each column value as string for pattern matching
	 * @return: True if the pattern is matched and false otherwise
	 */
	private boolean patternMatch(String colValue) {

		if (colValue != null) {
			if (phoneMatch(colValue)) {
				return true;
			} else if (emailMatch(colValue)) {
				return true;
			}
			else if (ipv4v6Match(colValue)) {
				return true;
			}
			else if (pancardMatch(colValue)) {
				return true;
			}
			else if (SSNMatch(colValue)) {
				return true;
			}
			else if (creditCardMatch(colValue)) {
				return true;
			}
			else if (bankMatch(colValue)) {
				return true;
			} 
			return false;
		} else {
			return false;
		}
	}

	
	/**
	 * This method performs the header matching for column names
	 * 
	 * @param: Accepts each column name as string for header name matching
	 * @return: True if the header is matched and false otherwise
	 */
	Boolean matchHeader(String colName) {

		if (colName.toLowerCase().matches("dob|date of birth|gender")) {
			return true;
		} else
			return false;

	}

	
	/**
	 * This API will identify the PII fields and returns
	 * column name and its corresponding PII values (yes/no) as map
	 * 
	 * @param: Accepts column header and column data as input
	 * @return: Map which has column name and PII (Yes/No)
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public Map<String, String> findPII(Map<String, String> columnHeaderMap,
			List<List<String>> columnDataList)  {
		
		logger.info("findPII api has started execution.");
		ExecutorService executor = Executors.newFixedThreadPool(columnDataList.size());
		List<Future<Map<String, String>>> list = new ArrayList<>();
		Iterator<String> it = columnHeaderMap.keySet().iterator();
		
		Map<String,String> hMap=new LinkedHashMap<>();//for header match
		for (int colNo = 0; colNo < columnHeaderMap.size(); colNo++) {
			String colName = it.next();
			Boolean matched = matchHeader(colName);
			if (matched) {
				hMap.put(colName, "Y");
			} else {
				List<String> columnList = columnDataList.get(colNo);
				Future<Map<String, String>> future = executor.submit(new PIIDetection(columnList, colName));
				list.add(future);
			}
		}

		Map<String,String> thMap=new LinkedHashMap<>();//this map will contain only column name for which PII match 
		for (Future<Map<String, String>> fut : list) {
			try {
				
				thMap.putAll(fut.get());
			} catch (Exception e) {
				e.printStackTrace();
				//logger.error("Thread exception for "+fut.get());
			}
		}
		executor.shutdown();
		logger.info("findPII api has finished execution.");
		
		hMap.putAll(thMap); //combined header matched column and thMap
		Map<String, String> outputMap = new LinkedHashMap<String, String>();
		for(Map.Entry<String,String> entry :columnHeaderMap.entrySet()) {
			String key=entry.getKey();
			 if(hMap.containsKey(key)){
				 outputMap.put(key, "Y"); 
			 }
			 else{
				 outputMap.put(key, "N");
			 }
		}

		return outputMap;
	}

	
	/**
	 * This method will be called for each thread with each column data
	 * 
	 * @param: Accepts column header and column data as input
	 * @return: Map which has column name and PII (Yes/No)
	 */	
	@Override
	public Map<String, String> call() {
		
		Map<String, String> tmpMap=new LinkedHashMap<String, String>();
		Iterator<String> it = column.iterator();
		while (it.hasNext()) {
			String colValue = it.next();
			if (colValue == null){
				continue;
			}
			Boolean matched = patternMatch(colValue.trim());
			if (matched) {
				tmpMap.put(colName, "Y");
				return tmpMap;
			}
		}
		return tmpMap;
	}

}
