package com.itc.zeas.validation.rule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.MappingJsonFactory;


public class JsonColumnValidatorParser {
	
	
	public Map<Integer,List<ValidationAttribute>> JsonParser(String jsonValues){

		List<String> validatorList=new ArrayList<>();
		Map<Integer,List<ValidationAttribute>> colNameAndValues=new LinkedHashMap<>();
		validatorList=DataValidatorUtility.getValidatorList();
		
//		for(ValidatorEnumNames name:ValidatorEnumNames.values()){
//			validatorList.add(name.toString());
//		}
		System.out.println(validatorList);
		JsonParser jp=null;
		try {
		JsonFactory f = new MappingJsonFactory();
	    jp= f.createJsonParser(jsonValues);
		JsonToken current;
		current = jp.nextToken();
		if (current != JsonToken.START_OBJECT) {
			System.out.println("Error: root should be object: quiting.");
		
		}
		
		int lineCount=0;
			while (jp !=null && jp.nextToken() != JsonToken.END_OBJECT) {
				//System.out.println("*********************************************");
				current = jp.nextToken();
				if (current == JsonToken.START_ARRAY) {
					// For each of the records in the array
					while (jp !=null && jp.nextToken() != JsonToken.END_ARRAY) {
						// read the record into a tree model,
						// this moves the parsing position to the end of it
						JsonNode node = jp.readValueAsTree();
						//System.out.println("##########################################");
						
						// And now we have random access to everything in the
						// object
						//ValidationAttribute validationAttribute=null;
						Iterator<String> fieldNames = node.getFieldNames();
						String colName="";
						//String validatorType="";
						Map<String,String> VNameAndValue= new HashMap<>();
						Map<String,String> ValidatorVsActualValidator= new HashMap<>();
						String dataTypeValue="";
						while (fieldNames.hasNext()) {
							
							String fName = fieldNames.next();
							String value = "";
							String keyName = fName.trim();
							if (node.get(fName).isTextual()) {
								value = node.get(fName).getTextValue();
							} else if (node.get(fName).isInt()) {
								Integer ival = node.get(fName).getIntValue();
								value = ival.toString();
							} else if (node.get(fName).isDouble()) {
								Double dval = node.get(fName).getDoubleValue();
								value = dval.toString();
							} else if (node.get(fName).isLong()) {
								Long lval = node.get(fName).getLongValue();
								value = lval.toString();
							} else if (node.get(fName).isBoolean()) {
								Boolean lval = node.get(fName).getBooleanValue();
								value = lval.toString();
							}
							//start
							if(keyName.equalsIgnoreCase("dataType")) {
								dataTypeValue=value;
							}
							
							//end
							if(validatorList.contains(keyName)) {
								
								String validtorName=getValidatorName(dataTypeValue,keyName);							
								VNameAndValue.put(validtorName, value);
								//validatorType=keyName;
								ValidatorVsActualValidator.put(validtorName, keyName);
							}
							else if(keyName.equalsIgnoreCase("Name")){
								colName=value;
							}

						}
						
						List<ValidationAttribute> validationAttributes=new ArrayList<>();
						 for(Entry<String,String> entry : VNameAndValue.entrySet()) {
							 ValidationAttribute attribute= new ValidationAttribute();
							 attribute.setColumnName(colName);
							 String validationName=entry.getKey();
							 DataValidation validationObject=DataValidatorFactory.getValidationInstance(validationName);
							 attribute.setValidationObject(validationObject);
							 attribute.setValidationValue(entry.getValue());
							 attribute.setDatatype(dataTypeValue);
							 attribute.setValidatorType(ValidatorVsActualValidator.get(validationName));
							 validationAttributes.add(attribute);
						 }
						colNameAndValues.put(lineCount, validationAttributes);
						 lineCount++;
						 
						//System.out.println("************************end of line***********************");
					}
					
					
				} else {
					//System.out.println("Error: records should be an array: skipping.");
					jp.skipChildren();
				}
				
			}
			
			
	}
	catch(Exception e){
		e.printStackTrace();
		try {
			if(jp!=null)
			 jp.close();
		} catch (IOException e1) {
		}
	 }
	
		 return colNameAndValues;
	}
	
	
	/*
	 * remove the invalid validator on columns
	 */
	public Map<Integer, List<ValidationAttribute>> getActualValidatorList(
			Map<Integer, List<ValidationAttribute>> colValidatorMap) {

		for (Entry<Integer, List<ValidationAttribute>> entry : colValidatorMap
				.entrySet()) {
			List<ValidationAttribute> attrValues = entry.getValue();
			List<ValidationAttribute> list = new ArrayList<>();
			for (ValidationAttribute attr : attrValues) {
				if (attr.getValidationObject() instanceof InvalidTypeValidator) {
					list.add(attr);
				}
			}
			attrValues.removeAll(list);
		}
		return colValidatorMap;
	}
	
	
	private String getValidatorName(String dataType, String validatorType) {

		String validatorName = "InvalidTypeValidator";
		Map<String,String> intValidatorList=DataValidatorUtility.getIntValidatorList();
		Map<String,String> stringValidatorList=DataValidatorUtility.getStringValidatorList();
		//IntFixedLenghtValidator,IntRangeValidator,StringRangeValidator,StringFixedLengthValidator,BlackListValidator
		//,WhilteListValidator,NotNullValidator,PiiRule,DoubleRangeValidator,RegXValidator,DateRangeValidator
		switch (validatorType) {
		
		case "Regex" :
			validatorName="RegXValidator";
			break;
			
		case "Range" :
			if(dataType.equalsIgnoreCase("int") || dataType.equalsIgnoreCase("long"))
				validatorName=intValidatorList.get(validatorType);
			else if(dataType.equalsIgnoreCase("string"))
				validatorName=stringValidatorList.get(validatorType);
			else if(dataType.equalsIgnoreCase("double"))
				validatorName="DoubleRangeValidator";
			else if(dataType.equalsIgnoreCase("date"))
				validatorName="DateRangeValidator";
			break;
			
		case "Fixed Length" :
			if(dataType.equalsIgnoreCase("int") || dataType.equalsIgnoreCase("long"))
				validatorName=intValidatorList.get(validatorType);
			else if(dataType.equalsIgnoreCase("string"))
				validatorName=stringValidatorList.get(validatorType);
			break;
		
		case "White List" :
			validatorName="WhilteListValidator";
			break;
			
		case "Black List" :
			validatorName="BlackListValidator";
			break;
			
		case "Confidential" :
			validatorName="PiiRule";
			break;
			
		case "Strict Validation" :
			validatorName="NotNullValidator";
			break;
		
		}

		return validatorName;
	}
	
	
}






