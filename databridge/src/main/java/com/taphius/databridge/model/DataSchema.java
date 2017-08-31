package com.taphius.databridge.model;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.taphius.validation.rule.ValidationRule;
import com.taphius.validation.rule.ValidationRuleFactory;

/**
 * POJO to represent dataschema entity.
 * This basically defines the schema of any dataset.
 * @author 16795
 *
 */
public class DataSchema {
    
    private String name;
    private String description;
    /**
     * This List holds set of attributes/columns that makeup this schema definition.
     */
    private List<SchemaAttributes> dataAttribute;
    private String query;
    private DatasourceFileDetails fileData;
    
	public DatasourceFileDetails getFileData() {
		return fileData;
	}
	public void setFileData(DatasourceFileDetails fileData) {
		this.fileData = fileData;
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	public String getDescription() {
        return description;
    }
    public void setDescription(String description) {
        this.description = description;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }   
    
    public static void main(String[] args) {
        String json ="{\"name\":\"Nisi_auth_log\",\"type\":\"DataSchema\",\"dataAttribute\":[{\"Name\":\"id\",\"dataType\":\"int\"},{\"Name\":\"user_id\",\"dataType\":\"string\"}],\"dataSchemaType\":\"Automatic\",\"description\":\"Nisi_auth_log\",\"fileData\":{\"dbType\":\"mysql\",\"port\":\"3306\",\"rowDeli\":\"tab\",\"colDeli\":\"tab\",\"format\":\"RDBMS\",\"tableName\":\"auth_log\",\"hostName\":\"10.6.116.134\",\"dbName\":\"zeas\",\"userName\":\"zeas\",\"password\":\"zeas\"},\"query\":\"select id,user_id from auth_log\"}";
      Gson gSon =new GsonBuilder().create();
       
        DataSchema schema  = gSon.fromJson(json, DataSchema.class);
        StringBuilder compositeKey = new StringBuilder();
        for(SchemaAttributes attributes:schema.getDataAttribute()){
        	/*if(attributes.getPrimary()!=null){
    		if(attributes.getPrimary().equalsIgnoreCase("yes")){
    		compositeKey.append(attributes.getName()+",");
    		}
    		}*/
        	System.out.println(attributes.getName()+":"+attributes.getDataType());
    	}
    /*	if(compositeKey.length()>0){
    		compositeKey.delete((compositeKey.length()-1), compositeKey.length());
    	}
    System.out.println(compositeKey);*/
    System.out.println(schema.getQuery());
    System.out.println(schema.getFileData().getHostName());
    }
    public List<SchemaAttributes> getDataAttribute() {
        return dataAttribute;
    }
    public void setDataAttribute(List<SchemaAttributes> dataAttribute) {
        this.dataAttribute = dataAttribute;
    }
   
    

}
