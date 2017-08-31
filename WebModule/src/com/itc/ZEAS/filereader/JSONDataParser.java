package com.itc.zeas.filereader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.MappingJsonFactory;

public class JSONDataParser {

	public Map<String,String> JsonParser(String jsonValues){

		Map<String,String> colNameAndValues=new HashMap<>();
		
//		for(ValidatorEnumNames name:ValidatorEnumNames.values()){
//			validatorList.add(name.toString());
//		}
		JsonParser jp=null;
		try {
		JsonFactory f = new MappingJsonFactory();
	    jp= f.createJsonParser(jsonValues);
		JsonToken current;
		current = jp.nextToken();
		if (current != JsonToken.START_OBJECT) {
			System.out.println("Error: root should be object: quiting.");
		
		}
		
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
							
							 if(keyName.equalsIgnoreCase("Name")){
								colName=value;
							}
						}
						
						colNameAndValues.put(colName, dataTypeValue);
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
	
}


