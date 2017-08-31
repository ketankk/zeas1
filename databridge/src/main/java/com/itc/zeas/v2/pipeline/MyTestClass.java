package com.itc.zeas.v2.pipeline;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MyTestClass {

	public static void main(String[] args) {
		Date now = new Date();
		System.out.println("date--> " + now);
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		String s = df.format(now);
		String result = s.substring(0, 26) + ":" + s.substring(27);
		System.out.println("result--> " + result);

	}

}
