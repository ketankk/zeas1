package com.taphius.validation.rule;

public class PIIRule {
    
    private String operation;
    
    private String remove(String value){
        return "";
    }
    
    private String obfuscate(String value){
        return value+"Obfuscated";
    }
    
    public String applyRule(String value){
        if(operation.equalsIgnoreCase("remove")){
           return remove(value);
        }else 
          return obfuscate(value);            
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

}
