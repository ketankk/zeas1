package com.zdp.transformations.impl;

import java.io.Serializable;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Group extends AbstractTransformations implements Serializable {

	/**
	 * Performs Group by transformation.
	 * input parameters should be input, output, schema, group by columns, aggregation type with field name
	 */
	private static final long serialVersionUID = 1L;
	static Logger logger = Logger.getLogger(Group.class);
	public String[] args;

	public Group(String[] arg) {
		args = arg;
	}

	public void execute() {

		// args[0] - type of transformation
		// args[1] - schema of file
		// args[2] - inputPath;outputPath;groupedcolumn;aggregationfield:aggregationType(,)
        try{
		String[] params = args[2].split(";");
		JavaSparkContext sc = new JavaSparkContext();
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		JavaRDD<String> inputDataRDD = sc.textFile(params[0]);
		// inputDataRDD.saveAsTextFile(params[1]);
		String[] schema = args[1].split(",");
		DateFormat df = DateFormat.getDateInstance();
		LinkedHashMap<Integer, String> hm1 = new LinkedHashMap<Integer, String>();
		for (int i = 0; i < schema.length; i++) {
			hm1.put(i, schema[i]);
		}
		logger.info("tree map is:"+hm1);
		//creating schema for the dataframe
		String schemaString = "";
		for (Map.Entry m : hm1.entrySet()) {
			String temp = (String) m.getValue();
			schemaString = schemaString + temp + ",";
		}
		final String scStr = schemaString;
		final String[] fieldsReference = scStr.substring(0, scStr.length() - 1).split(
				",");

		final List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName : schemaString.substring(0,
				schemaString.length() - 1).split(",")) {
			String field = fieldName.split(":")[0];
			String dataType = fieldName.split(":")[1];
			logger.info("fields are " + field + "+++++++++++" + dataType);
			if (dataType.equalsIgnoreCase("String")) {

				fields.add(DataTypes.createStructField(field,
						DataTypes.StringType, true));
			} 
			else if (dataType.equalsIgnoreCase("date")) {
				fields.add(DataTypes.createStructField(field,DataTypes.StringType,true));
			} 
			else  {
				fields.add(DataTypes.createStructField(field,
						DataTypes.DoubleType, true));
			}
			
		}
        // creating ROWRDD for the dataframe
		JavaRDD<Row> rowRDD = inputDataRDD.map(new Function<String, Row>() {
			public Row call(String record) throws Exception {
				String[] fields = record.split(",");
				//Changed from fileds.length to fieldsReference, since in ingested file we are adding 2 extra fields
				// ie timestamp and source file name to handle metadata 
				Object[] tmp = new Object[fieldsReference.length];
				for (int i = 0; i < fieldsReference.length; i++) {
					String[] temp = fieldsReference[i].split(":");
					String field = temp[0];
					String dataType = temp[1];
					logger.info("field[i]================"+fields[i]);
					if(fields[i].isEmpty()||fields[i].equalsIgnoreCase("NA")||fields[i].equalsIgnoreCase("null"))
						continue;
					if (dataType.equalsIgnoreCase("String")) {

						tmp[i] = fields[i];
					} else if (dataType.equalsIgnoreCase("date")) {
						//DateFormat df = DateFormat.getDateInstance();
						tmp[i] = fields[i];
					} 
					else  {
						tmp[i] = new Double(fields[i]).doubleValue();
					}
					
				}
				return RowFactory.create(tmp);
			}
		});
		StructType schemaFields = DataTypes.createStructType(fields);
		//created dataframe
		DataFrame dataFrame = sqlContext.createDataFrame(rowRDD, schemaFields);
		//register temporary table
		dataFrame.registerTempTable("Temptable");
		String groupedColumns = params[2];
		String aggregation = ",";
		
		if(!(params[3].equalsIgnoreCase("null"))){
		String aggregationFieldsAndTypes[] = params[3].split(",");
		//framing aggregations and fields as string
		for (int i = 0; i < aggregationFieldsAndTypes.length; i++) {
			String[] aggregationFieldAndType = aggregationFieldsAndTypes[i]
					.split(":");
			String field = aggregationFieldAndType[0];
			String type = aggregationFieldAndType[1];
			aggregation += type + "(" + field + "),";
		}
		}
		
		aggregation = aggregation.substring(0,aggregation.length() -1);
		
		
		logger.info("Qeury is ***********" + "select " + groupedColumns + 
				 aggregation
				+ " from Temptable group by " + groupedColumns);
		// executing the query of group by aggregationColumns and aggregations

		DataFrame results = sqlContext.sql("select " + groupedColumns + " "
				+ aggregation
				+ " from Temptable group by " + groupedColumns);
		//collecting the final results and saving in HDFS.
		List<String> finalResults = results.javaRDD().map(new Function<Row, String>() {
			  public String call(Row row) {
				 // logger.info("row is ************"+ row.toString().substring(1, row.toString().length()-1));
			    return row.toString().substring(1, row.toString().length()-1);
			  }
			}).collect();
		sc.parallelize(finalResults,1).saveAsTextFile(params[1]);
		sc.close();
        }
        catch(Exception e){
        	logger.info("problem occured in group class");
        	e.printStackTrace();
        }
		
	}

}
