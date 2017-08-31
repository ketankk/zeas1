package com.taphius.validation.rule;

import org.apache.log4j.Logger;
/**
 * Class for validating integer values.
 * Checks if the given value is in range of min and max.
 * @author 16795
 *
 */
public class IntValidator implements ValidationRule {

    private int minVal;
    private int maxVal;
    private int valueFound;
    private String column;
    private String errorMsg = "";
    private PIIRule pii;
    private boolean enabled;
    public static Logger LOG = Logger.getLogger(IntValidator.class);
    public static final String RULE_NAME = "Integer validation";


    public IntValidator(int min, int max, String col){
        this.minVal = min;
        this.maxVal = max;
        this.column = col;
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

    @Override
    public boolean validate(Object obj) {
        boolean valid = false;
        StringBuilder builder = new StringBuilder();

        try{
//            valueFound = Integer.parseInt(obj.toString());
            valid = (valueFound >= this.minVal && 0!= maxVal && valueFound <= this.maxVal);
            if(!valid){
                builder.append(RULE_NAME);
                builder.append(",");
                builder.append(getExpected());
                builder.append(",");
                builder.append(valueFound);
                builder.append(",");
                builder.append(column);
                builder.append(",");
                builder.append(ValidationRuleFactory.getTimeStamp());
                errorMsg = builder.toString();
            }
        }
        catch(NumberFormatException nfe){
            LOG.error("Error converting value to integer."+nfe.getMessage());
        }
        return valid;
    }
    
    public boolean typeCheck(Object obj){
        try{
            valueFound = Integer.parseInt(obj.toString());
        }
            catch(NumberFormatException nfe){
                LOG.error("Error converting value to integer."+nfe.getMessage());
                StringBuilder builder = new StringBuilder();
                builder.append(RULE_NAME);
                builder.append(",");
                builder.append("Integer");
                builder.append(",");
                builder.append(obj.toString());
                builder.append(",");
                builder.append(column);
                builder.append(",");
                builder.append(ValidationRuleFactory.getTimeStamp());
                errorMsg = builder.toString();
                return false;
          }
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
        return minVal+" - "+maxVal;
    }

    @Override
    public String getColumn() {
        return this.column;
    }

    @Override
    public String getValue() {
       return ""+valueFound;
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
