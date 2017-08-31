package com.itc.zeas.validation.rule;

import lombok.Data;

@Data
public class ValidationAttribute {


    private DataValidation validationObject;
    private String validationValue;
    private String columnName;
    private String datatype;
    private String validatorType;


    @Override
    public String toString() {
        // TODO Auto-generated method stub
        String objectInfo = "Invalid validator selection";
        if (validationObject != null) {
            objectInfo = validationValue.toString();
        }
        return " validatorType:" + validatorType
                + " ,validationValues: " + validationValue + ",colname: "
                + columnName + ", datatype:" + datatype;
    }


}
