package com.taphius.validation.rule;
/**
 * Validates string for min and max length limits.
 * @author 16795
 *
 */
public class StringValidator implements ValidationRule {

    private int minLen;
    private int maxLen;
    private String valueFound;
    private String errorMsg = "";
    private String column;
    private PIIRule pii;
    private boolean enabled;
    public static final String RULE_NAME = "String validation";
    
    public StringValidator(int min, int max, String col) {        
        this.minLen = min;
        this.maxLen = max;
        this.column = col;
    }

    @Override
    public boolean validate(Object obj) {
        valueFound = obj.toString();
        int length = valueFound.length();
        StringBuilder builder = new StringBuilder();
        boolean valid = length >= minLen && 0 != maxLen && length <= maxLen ;
        if(!valid){
            builder.append(RULE_NAME);
            builder.append("\t");
            builder.append(getExpected());
            builder.append("\t");
            builder.append(valueFound);
            builder.append("\t");
            builder.append(column);
            builder.append("\t");
            builder.append(ValidationRuleFactory.getTimeStamp());
           errorMsg = builder.toString();
        }
       return valid;
    }
    
    public boolean typeCheck(Object obj){       
          return true;
    }

    @Override
    public String getError() {
        return errorMsg;
    }

    @Override
    public String getRuleName() {
        return RULE_NAME;
    }

    @Override
    public String getExpected() {
        return null;
    }

    @Override
    public String getColumn() {
        return this.column;
    }

    @Override
    public String getValue() {
        return "Length is - "+valueFound.length();
    }

    @Override
    public PIIRule getPIIRule() {
        return this.pii;
    }

    @Override
    public void setPIIRule(PIIRule pii) {
        this.pii = pii;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void enableRule(String isEnabled) {
        if("YES".equalsIgnoreCase(isEnabled)){
            enabled = true;
        }else {
            enabled = false;
        }
    }  

   
}
