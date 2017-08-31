package com.taphius.validation.rule;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.taphius.databridge.model.SchemaAttributes;
/**
 * A factory class to generate {@link ValidationRule} instances,
 * depending on the data type of {@link SchemaAttributes}
 * @author 16795
 *
 */
public class ValidationRuleFactory {
    /**
     * Method creates instance of {@link ValidationRule} depending on the type.
     * @param attr {@link SchemaAttributes} instance
     * @return instance implementing {@link ValidationRule}
     */
    public static ValidationRule getRuleInstance(SchemaAttributes attr){
        ValidationRule rule =  null;
        String type = attr.getDataType();
        
        switch (type) {
        case "int":
            rule = new IntValidator(attr.getMinVal(), attr.getMaxVal(), attr.getName());
            break;
        case "varchar":
            rule = new StringValidator(attr.getMinlen(), attr.getMaxlen(), attr.getName());
            break;
        case "long":
            rule = new LongValidator();
            break;
        case "float":
            rule = new FloatValidator();
            break;
        case "date":
            rule = new DateValidator();
            break;
        case "time":
            rule = new TimeValidator();
            break;
        default:
            break;
        }if(rule!=null) {
            rule.setPIIRule(ValidationRuleFactory.getPIIRule(attr.getActive(), attr.getPiirule()));
            rule.enableRule(attr.getValRule());
        }
        return  rule;
        
    }
    
    private static PIIRule getPIIRule(String isPII, String operation){
        
        PIIRule pii = null;
        if("true".equalsIgnoreCase(isPII)){
            pii = new PIIRule();
            pii.setOperation(operation);
        }
        return pii;
    }
    
    public static String getTimeStamp(){
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }

}
