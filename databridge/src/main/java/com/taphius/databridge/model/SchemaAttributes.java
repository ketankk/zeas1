package com.taphius.databridge.model;
/**
 * Class defines the attributes for the each column in schema.
 * This has validation rule definition if applicable,
 * and also a way to mark a column as PII.
 * @author 16795
 *
 */

//TODO move this to DAO
public class SchemaAttributes {
    
    private String Name;
    private String dataType;
    private int minVal;
    private int maxVal;
    private int minlen;
    private int maxlen;
    private String active;
    private String piirule;
    private String valRule;
    private String Primary;
    
    public SchemaAttributes(String name, String type) {
        this.Name = name;
        this.dataType = type;
    }
    
    public SchemaAttributes(){
        
    }
    public String getName() {
        return Name;
    }
    public void setName(String name) {
        this.Name = name;
    }
    public int getMinVal() {
        return minVal;
    }
    public void setMinVal(int minVal) {
        this.minVal = minVal;
    }
    public int getMaxVal() {
        return maxVal;
    }
    public void setMaxVal(int maxVal) {
        this.maxVal = maxVal;
    }
    public int getMinlen() {
        return minlen;
    }
    public void setMinlen(int minlen) {
        this.minlen = minlen;
    }
    public int getMaxlen() {
        return maxlen;
    }
    public void setMaxlen(int maxlen) {
        this.maxlen = maxlen;
    }


	public String getPrimary() {
		return Primary;
	}

	public void setPrimary(String primary) {
		Primary = primary;
	}

	public String getDataType() {
        return dataType;
    }
    public void setDataType(String dataType) {
        this.dataType = dataType;
    }   
    public String getPiirule() {
        return piirule;
    }
    public void setPiirule(String piirule) {
        this.piirule = piirule;
    }
    
    public String getActive() {
        return active;
    }
    public void setActive(String active) {
        this.active = active;
    }
    public String getValRule() {
        return valRule;
    }
    public void setValRule(String valRule) {
        this.valRule = valRule;
    }
 

}
