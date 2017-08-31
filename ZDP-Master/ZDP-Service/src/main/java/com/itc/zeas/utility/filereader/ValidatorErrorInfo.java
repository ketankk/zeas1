package com.itc.zeas.utility.filereader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.itc.zeas.validation.rule.DataValidatorUtility;
import com.itc.zeas.validation.rule.InvalidTypeValidator;
import com.itc.zeas.validation.rule.JsonColumnValidatorParser;
import com.itc.zeas.validation.rule.ValidationAttribute;

public class ValidatorErrorInfo {
	
	
	public List<String> getErrors(String  json) {

		//String json = entity.getJsonblob();

		Map<Integer, List<ValidationAttribute>> colValidatorMap;
		JsonColumnValidatorParser attrParser = new JsonColumnValidatorParser();
		colValidatorMap = attrParser.JsonParser(json);
		// System.out.println(colValidatorMap);
		List<String> errorList = new ArrayList<>();
		for (Entry<Integer, List<ValidationAttribute>> entry : colValidatorMap
				.entrySet()) {
			List<ValidationAttribute> attrValues = entry.getValue();
			List<ValidationAttribute> list = new ArrayList<>();
			for (ValidationAttribute attr : attrValues) {
				String error = "";
				if (attr.getValidationObject() instanceof InvalidTypeValidator) {
					attr.getValidationObject().isValidate(attr, null);

					error = DataValidatorUtility.getErrorMsgForInvalidValidator(attr.getValidatorType()
									+ ","+ attr.getDatatype());
					if (!(error != null && error.isEmpty())) {
						errorList.add(attr.getValidatorType() + "|"+ attr.getColumnName() + "|" + error);
					}
				} else {
					error = DataValidatorUtility.getErrorMsgForValidValidator(
							attr.getValidatorType(), attr.getValidationValue());
					if (!(error != null && error.isEmpty())) {
						errorList.add(attr.getValidatorType() + "|"+ attr.getColumnName() + "|" + error);
					}
				}
			}
			attrValues.removeAll(list);
		}
		return errorList;
	}

}
