package com.itc.zeas.mr;

import java.text.DecimalFormat;

public class GenerateValueForDataType {

	public String getDummyVlaueForDataType(String dataType) {

		String result = "";
		switch (dataType) {
		case "string":
			StringBuilder builder = new StringBuilder("");
			for (int i = 1; i <= 8; i++) {
				Long temp = Math.round(Math.random() * 10);
				builder.append((char) (temp + 97));
			}
			result = builder.toString();
			break;

		case "int":
			Long intData = Math.round(Math.random() * 100);
			result = intData.toString();
			break;

		case "long":
			Long longData = Math.round(Math.random() * 10000);
			result = longData.toString();
			break;

		case "double":
			Double value = Math.random() * 10000;
			DecimalFormat df = new DecimalFormat("####0.000");
			result = df.format(value);
			break;
		case "date":

			long yyyy = Math.round(Math.random() * 10 + 2010);
			long month = Math.round(Math.random() * 10 + 1);
			String mm = "";
			if (month < 10) {
				mm = "0" + month;
			} else {
				mm = "" + month;
			}
			long date = Math.round(Math.random() * 28);
			String dd = "";
			if (date < 10) {
				dd = "0" + date;
			} else {
				dd = "" + date;
			}

			result = "" + yyyy + "-" + mm + "-" + dd;
			break;
		case "timestamp":

			long y1 = Math.round(Math.random() * 10 + 2010);
			long month1 = Math.round(Math.random() * 10 + 1);
			String mm1 = "";
			if (month1 < 10) {
				mm1 = "0" + month1;
			} else {
				mm1 = "" + month1;
			}
			long date1 = Math.round(Math.random() * 28);
			String dd1 = "";
			if (date1 < 10) {
				dd1 = "0" + date1;
			} else {
				dd1 = "" + date1;
			}

			result = "" + y1 + "-" + mm1 + "-" + dd1 + " 08:23:22";
			break;
		default:

		}
		return result;
	}

}
