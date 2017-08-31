package com.taphius.validation.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import com.itc.zeas.validation.rule.DataTypeCheckUtility;
import com.itc.zeas.validation.rule.DataValidation;
import com.itc.zeas.validation.rule.JSONDataParser;
import com.itc.zeas.validation.rule.JsonColumnValidatorParser;
import com.itc.zeas.validation.rule.ValidationAttribute;
import com.taphius.validation.rule.ValidationRule;


public class AirportValidationMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    Pattern pattern = null;
    MultipleOutputs<NullWritable, Text> multipleOutputs;
    List<ValidationRule> rules = new ArrayList<ValidationRule>();
    String outPutPath = "";
    String batchId = "";
    String dataSchemaId = "";
    JSONDataParser dataTypeparser=new JSONDataParser();
    Map<Integer,List<ValidationAttribute>> colValidatorMap;
    Map<String,String> columnNameAndDataType;
    Map<Integer,String> dataType;
	private static String ingestionTime="";
	private static String fileName=""; 
    public static Logger LOG = Logger.getLogger(AirportValidationMapper.class);
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
       
        Configuration conf = context.getConfiguration();
        outPutPath = conf.get("job.output.path");
        dataSchemaId = conf.get("dataSchema.value");
        batchId= conf.get("batch.id");
        ingestionTime=conf.get("ingestion.time");
        FileSplit fileSplit=(FileSplit) context.getInputSplit();
        try{
        fileName=fileSplit.getPath().getName().substring(13, fileSplit.getPath().getName().length());
        }catch(Exception e){
        	fileName="MySQl";
        }
        LOG.info("File Name in Mapper :"+fileName );
        LOG.info("DataSchema value --"+dataSchemaId); 
        System.out.println("dataSchemaID :"+dataSchemaId);
//        String json ="{\"sourcerType\":\"int\",\"id\":7016,\"name\":\"testdata\",\"description\":\"testdata description\",\"dataSourcerId\":\"testdata\",\"attributes\":[{\"dataType\":\"int\",\"isEnabled\":\"YES\",\"name\":\"id\",\"description\":\"Unique Id\",\"PII\":true,\"piirule\":\"Obfuscate\",\"maxVal\":9000,\"active\":true},{\"dataType\":\"varchar\",\"valRule\":\"YES\",\"name\":\"name of airport\",\"description\":\"Name of the airport\",\"PII\":false,\"piirule\":\"Remove\",\"minlen\":2,\"maxlen\":100,\"active\":true}]}";
       // String json = conf.get("dataSchema.JSON");
       // String json="{\"dataSourcerId\":\"testdata\",\"dataAttribute\":[{\"Name\":\"id\",\"dataType\":\"int\"},{\"Name\":\"name\",\"dataType\":\"string\"}," +
    	//		"{\"Name\":\"type\",\"dataType\":\"string\"},{\"Name\":\"price\",\"dataType\":\"int\",\"IntRangeValidator\":\"5000,15000\"}],\"d2222\":\"testdata\"}";
        
       // String json="{\"dataSourcerId\":\"testdata\",\"dataAttribute\":[{\"Name\":\"id\",\"dataType\":\"int\"},{\"Name\":\"name\",\"dataType\":\"string\",\"StringFixedLengthValidator\":\"4\",\"RegXValidator\":\"yy.*\"}," +
    	//		"{\"Name\":\"type\",\"dataType\":\"string\"},{\"Name\":\"price\",\"dataType\":\"int\",\"IntRangeValidator\":\"5000,15000\"}],\"d2222\":\"testdata\"}";
        
        String json = conf.get("dataSchema.JSON");
  /*      String json="{\"dataSourcerId\":\"testdata\",\"dataAttribute\":[{\"Name\":\"id\",\"dataType\":\"int\"}," +
        		"{\"Name\":\"doj\",\"dataType\":\"string\"}," +
        		"{\"Name\":\"empid\",\"dataType\":\"int\",\"Confidential\":true}," +
        		"{\"Name\":\"price\",\"dataType\":\"double\",\"Range\":\"180-200\"}," +
        		"{\"Name\":\"test1\",\"dataType\":\"string\"},{\"Name\":\"test2\",\"dataType\":\"string\"}," +
        		"{\"Name\":\"test3\",\"dataType\":\"string\"}," +
        		"{\"Name\":\"test4\",\"dataType\":\"string\",\"RegEx\":\"C.*\",\"Fixed Length\":8}," +
        		"{\"Name\":\"test5\",\"dataType\":\"string\"}],\"d2222\":\"testdata\"}";*/
        
        System.out.println("JSON.............:"+json);
        JsonColumnValidatorParser attrParser= new JsonColumnValidatorParser();
        Map<Integer,List<ValidationAttribute>> tmpColValidatorMap=attrParser.JsonParser(json); 
        colValidatorMap=attrParser.getActualValidatorList(tmpColValidatorMap);
        System.out.println("list of validtor");
        System.out.println("column count"+colValidatorMap.size());
        System.out.println("value:----"+colValidatorMap);
        
        // used to get column number with data type as a map.        
        columnNameAndDataType=dataTypeparser.JsonParser(json);
        dataType =DataTypeCheckUtility.getcolNumberAndDataTypeMap(columnNameAndDataType);
        
       /* try {
            json = DBUtility.getJSON_DATA(dataSchemaId);
        } catch (SQLException e) {
            LOG.error(e.getMessage());
        }   */    
      //  DataSourcerConfigDetails<DataSchema> parser = new DataSourcerConfigDetails<DataSchema>(DataSchema.class);
       
    //    DataSchema schema  = parser.getDSConfigDetails(json);
        multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
        
//       for (SchemaAttributes attr : schema.getDataAttribute()) {
//        rules.add(ValidationRuleFactory.getRuleInstance(attr));
//       }   
       LOG.info("Rules =="+rules);
        
    }
    @Override
    protected void map(LongWritable key, Text inputValue, Context context) throws java.io.IOException, java.lang.InterruptedException {
        String value=inputValue.toString();
        String columns[] = value.split(",",-1);
       // StringBuilder builder = new StringBuilder();
        boolean cleansed = true;
       // int i = 0;
        DataValidation lastValidatedType=null;
        //context.getCounter(AirportDataValidator.RECORDS_PROCESSED.TOTAL).increment(1);
        setCounterForProcessRecord(context, "TOTAL");
        if(columns.length != colValidatorMap.size()){
        	//System.out.println("input\n"+value.toString()+"\n************************************: not equal text lenth"+columns.length+": map size:"+colValidatorMap.size());
            cleansed = false;
            multipleOutputs.write(NullWritable.get(), new Text("Column mismatch,"+"exceeded column size: "+columns.length + ", schema size:"+colValidatorMap.size()+","+new TimeStamp(System.currentTimeMillis())+","+ingestionTime+","+fileName+","+"'"+inputValue.toString().replaceAll(",", "  ")+"'"), outPutPath+"/quarantine/part_"+batchId);
            //context.getCounter(AirportDataValidator.RECORDS_PROCESSED.QUARANTINE).increment(1);
            setCounterForProcessRecord(context, "noOfColumnMismatchFails");
           //to test// Integer.parseInt("adjadah");
        }  
        else{
		for (int count = 0; count < columns.length; count++) {
			if((colValidatorMap.get(count).size()==0)){
				if(("null".equalsIgnoreCase(columns[count]) || columns[count]==null || "na".equalsIgnoreCase(columns[count]) || columns[count].trim().isEmpty())){
					cleansed = true;	
					//context.getCounter(AirportDataValidator.RECORDS_PROCESSED.QUARANTINE).increment(1);
					//setCounterForProcessRecord(context, "DataTypeMismatch");
					//multipleOutputs.write(NullWritable.get(), new Text("DataType not matched.value is null/na/empty "+","+"expected datatype : "+dataType.get(count)+","+" Value found : "+columns[count]+","+(count+1)+","+new TimeStamp(System.currentTimeMillis())+","+ingestionTime+","+fileName+","+"'"+inputValue.toString().replaceAll(",", "  ")+"'"), outPutPath+"/quarantine/part_"+batchId);
					continue;
				}
			if(!DataTypeCheckUtility.checkType(dataType.get(count), columns[count])){
				cleansed = false;	
				//context.getCounter(AirportDataValidator.RECORDS_PROCESSED.QUARANTINE).increment(1);
				setCounterForProcessRecord(context, "DataTypeMismatch");
				multipleOutputs.write(NullWritable.get(), new Text("DataType not matched expected is ,"+"expected datatype : "+dataType.get(count)+","+" Value found : "+columns[count]+","+(count+1)+","+new TimeStamp(System.currentTimeMillis())+","+ingestionTime+","+fileName+","+"'"+inputValue.toString().replaceAll(",", "  ")+"'"), outPutPath+"/quarantine/part_"+batchId);
				break;
			  }
			}else {
				List<ValidationAttribute> validationRules = colValidatorMap.get(count);
				String validationName="";
				boolean isStrict=false;
				for (ValidationAttribute attribute : validationRules) {
					if(attribute.getValidatorType().equalsIgnoreCase("Strict Validation")){
						if(attribute.getValidationObject().isValidate(attribute,columns[count])==1){
						isStrict=true;
						}
					}
					
				}
					for (ValidationAttribute attributes : validationRules) {
						if(!attributes.getValidatorType().equalsIgnoreCase("Strict Validation")){
						//System.out.println("###############val:"+attribute+": value"+columns[count]);
						String validatorName=attributes.getValidationObject().getClass().getSimpleName();
						 if("PiiRule".equalsIgnoreCase(validatorName)) {
							 String maskedValues=attributes.getValidationObject().performMasking(attributes, columns[count]);
							 value=value.replaceAll(columns[count], maskedValues);
						   }
						 switch(attributes.getValidationObject().isValidate(attributes,columns[count])){
						 
						 case 0:
							 if(!isStrict){
								//Rule is not Strict so record goes to Quarantine and cleansed both.
							    cleansed=true;
								lastValidatedType=attributes.getValidationObject();
								validationName=attributes.getValidatorType();
								setCounterForProcessRecord(context, validationName);
							 	multipleOutputs.write(NullWritable.get(), new Text(lastValidatedType.getError()+","+new TimeStamp(System.currentTimeMillis())+","+ingestionTime+","+fileName+ ","+"'"+inputValue.toString().replaceAll(",", "  ")+"'"), outPutPath+"/quarantine/part_"+batchId);
							 }else{
								 //Rule is strict Only Quarantine 
								 cleansed = false;
								 lastValidatedType=attributes.getValidationObject();
								 validationName=attributes.getValidatorType();
							 }
							 break;
						 case -1:
							 // Value is Null/Empty so both Quarantine and cleansed.
							 	cleansed=true;
							 	lastValidatedType=attributes.getValidationObject();
								validationName=attributes.getValidatorType();
							 	setCounterForProcessRecord(context, validationName);
							 	multipleOutputs.write(NullWritable.get(), new Text(lastValidatedType.getError()+","+new TimeStamp(System.currentTimeMillis())+","+ingestionTime+","+fileName+ ","+"'"+inputValue.toString().replaceAll(",", "  ")+"'"), outPutPath+"/quarantine/part_"+batchId);
							 	break;
						 case  1:
							 //Cleansed.....
							    cleansed = true;
								break;
						 }
						 if(!cleansed){
							 break;
						 }
					}
					}
				if (!cleansed) {
		            //context.getCounter(AirportDataValidator.RECORDS_PROCESSED.QUARANTINE).increment(1);
		            setCounterForProcessRecord(context, validationName);
		            multipleOutputs.write(NullWritable.get(), new Text(lastValidatedType.getError()+","+new TimeStamp(System.currentTimeMillis())+","+ingestionTime+","+fileName+ ","+"'"+inputValue.toString().replaceAll(",", "  ")+"'"), outPutPath+"/quarantine/part_"+batchId);
		            break;
		        }
			}
		}
        }
        //Apply schema validation.Check if no.of columns is equal to 
        //columns in schema.
//        if(columns.length != colValidatorMap.size()){
//            cleansed = false;
//            multipleOutputs.write(NullWritable.get(), new Text("exceeded column size: "+columns.length + "than schema size:"+colValidatorMap.size()+","+value.toString()), outPutPath+"/quarantine/part");
//            context.getCounter(AirportDataValidator.RECORDS_PROCESSED.QUARANTINE).increment(1);
//           
//        }            
        
/*        for(i = 0 ; i < rules.size(); i++){
            String val = columns[i];
            //Perform Type Check 
            if(!rules.get(i).typeCheck(val)){
                cleansed = false;
                break;
            }
            //Apply validation rule check..
            if(rules.get(i).isEnabled()){
                
           
            if(!rules.get(i).validate(val)){                
                //builder.append(columns[i]);
              //  builder.append(",");
             //   continue;
                cleansed = false;
                break;
            }
            
            }
            //Apply PII rule check..
            if(rules.get(i).getPIIRule()!= null){
                
                val = rules.get(i).getPIIRule().applyRule(columns[i]);
            }
            builder.append(val);
            builder.append(",");
            continue;
        }*/
        
        if(cleansed){
        	//System.out.println("@@@@@@@@@@@@@@@@@@@ cleansed");
        	multipleOutputs.write(NullWritable.get(), new Text(value+","+ingestionTime+","+fileName),outPutPath+"/cleansed/part_"+batchId);
           // context.getCounter(AirportDataValidator.RECORDS_PROCESSED.CLEANSED).increment(1);
            setCounterForProcessRecord(context, "CLEANSED");
//            context.write(NullWritable.get(), value);
        }
        
    }

    @Override
    protected void cleanup(Context context) throws java.io.IOException, InterruptedException {
        multipleOutputs.close();
    }
    
    
    
    private void setCounterForProcessRecord(Context context,String validationName) {
    	
		switch (validationName) {

		case "TOTAL":
			 context.getCounter(AirportDataValidator.RECORDS_PROCESSED.TOTAL).increment(1);
			break;
		case "CLEANSED":
			 context.getCounter(AirportDataValidator.RECORDS_PROCESSED.CLEANSED).increment(1);
			break;
		case "Fixed Length":
			 context.getCounter(AirportDataValidator.RECORDS_PROCESSED.noOfFixedlLengthFails).increment(1);
			break;
		case "Range":
			 context.getCounter(AirportDataValidator.RECORDS_PROCESSED.noOfRangeFails).increment(1);
			break;
		case "Mandatory":
			 context.getCounter(AirportDataValidator.RECORDS_PROCESSED.noOfMandatoryFails).increment(1);
			break;
		case "White List":
			 context.getCounter(AirportDataValidator.RECORDS_PROCESSED.noOfWhiteListFails).increment(1);
			break;
		case "Black List":
			 context.getCounter(AirportDataValidator.RECORDS_PROCESSED.noOfBlackListFails).increment(1);
			break;
		case "Regex":
			 context.getCounter(AirportDataValidator.RECORDS_PROCESSED.noOfRegexFails).increment(1);
			break;
		case "DataTypeMismatch":
			 context.getCounter(AirportDataValidator.RECORDS_PROCESSED.noOfDataTypeMismatchFails).increment(1);
			break;
		case "noOfColumnMismatchFails":
			 context.getCounter(AirportDataValidator.RECORDS_PROCESSED.noOfColumnMismatchFails).increment(1);
			break;
				
		case "default":
			 context.getCounter(AirportDataValidator.RECORDS_PROCESSED.noOfotherFails).increment(1);
			break;

		}
    }
    
//    public String getSchemaFailureMsg(int expected, int found){
//        String msg = "Schema Validation ,"+expected+", "+found+", ,"+ValidationRuleFactory.getTimeStamp();
//        return msg;
//    }
}