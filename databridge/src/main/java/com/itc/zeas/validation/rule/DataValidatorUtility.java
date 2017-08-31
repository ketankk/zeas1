package com.itc.zeas.validation.rule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class DataValidatorUtility {
	
	public static List<String> getValidatorList(){
		String[] validatorList={"Regex","Range","White List","Black List", "Fixed Length","Confidential","Strict Validation"};
		return Arrays.asList(validatorList);
	}

	
	public static Map<String,String> getIntValidatorList(){
		Map<String,String> intValidatorMap=new HashMap<>();
		intValidatorMap.put("Range", "IntRangeValidator");
		intValidatorMap.put("Fixed Length", "IntFixedLenghtValidator");
		return intValidatorMap;
	}
	
	
	public static Map<String,String> getStringValidatorList(){
		Map<String,String> stringValidatorMap=new HashMap<>();
		stringValidatorMap.put("Range", "StringRangeValidator");
		stringValidatorMap.put("Fixed Length", "StringFixedLengthValidator");
		return stringValidatorMap;
	}
	
	public static  String getActualValidatorNameFromValidator(String validatorName){
		Map<String,String> validatorMap = new HashMap<>();
		//validatorMap.put(key, value)
		
		return null;
	}
	
	//IntFixedLenghtValidator,IntRangeValidator,StringRangeValidator,StringFixedLengthValidator,BlackListValidator
	//,WhilteListValidator,NotNullValidator,PiiRule,DoubleRangeValidator,RegXValidator
	
	
	public static String getErrorMsgForInvalidValidator(String errorMsg){
		
		Map<String,String> errorMsgMap=new HashMap<>();
		errorMsgMap.put("Fixed Length,double", "Fixed Length is not applicable for double datatype.");
		errorMsgMap.put("Fixed Length,date", "Fixed Length is not applicable for date datatype.");
		errorMsgMap.put("Fixed Length,timestamp", "Fixed Length is not applicable for timestamp datatype.");
		errorMsgMap.put("Range,timestamp", "Range is not applicable for timestamp datatype.");
		
		return errorMsgMap.get(errorMsg);
	}
	
	public static String getErrorMsgForValidValidator(String validatorType,
			String validationValue) {
		
		String error="";
		switch (validatorType) {

		case "Regex":
			try{
				Pattern.compile(validationValue);
				 
			}catch(Exception e){
				error="Not a valid input. Please use proper regex pattern.";
			}

			break;

		case "Range":
			//validationValue=validationValue.replaceAll("\\s+", "");
			try{
				if(validationValue.contains(":")){
			String[] values=validationValue.split(":");
			if(values.length==1){
                error="Not a valid input.only single input given, Please provide valid numeric range separated by colon(:)  e.g. 12:23 or 23.4:89.77 etc";
          }
          else if(values.length==2){
                       Double.parseDouble(values[0]);
                       Double.parseDouble(values[1]);
          }
          else if(values.length>2) {
                error="Not a valid input.More than two input given, Please provide two valid numeric values separated by colon(:)  e.g. 12:23 or 23.4:89.77 etc";
          }
          }else if((validationValue.contains("<") || validationValue.contains(">")) && validationValue.contains("=")){
                Double.parseDouble(validationValue.substring(2));
          }else if(validationValue.contains("<") || validationValue.contains(">")){
                Double.parseDouble(validationValue.substring(1));
          }
          else {
                error="Not a valid input.Please provide two valid numeric values separated by colon(:)  e.g. 12:23 or 23.4:89.77 etc";
          }
          }catch(Exception e1){
                error="Not a valid input.characters input given,Please provide two valid numeric values separated by colon(:)  e.g. 12:23 or 23.4:89.77 etc";
                }
          break;


		case "Fixed Length":
				try{
					Integer.parseInt(validationValue);
				}catch(Exception e){
				error="Not a valid input.characters input given,Please take single numeric values  like 9 or 67 or 100  etc.";
				}
			break;

		}
		return error;
	}
}
