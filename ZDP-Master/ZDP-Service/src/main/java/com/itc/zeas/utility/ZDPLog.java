package com.itc.zeas.utility;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class ZDPLog {

	String className;
	DateFormat dateFormat;

	ZDPLog(String className) {
		this.className = className;
		dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	}

	public static ZDPLog getZDPLog(String className) {

		ZDPLog log = new ZDPLog(className);
		return log;
	}

	// String sDate=dateFormat.format(System.currentTimeMillis());

	public String INFO(String message) {
		StringBuilder sb = new StringBuilder();
		sb.append(dateFormat.format(System.currentTimeMillis()));
		sb.append(" INFO " + className + ": " + message + "\n");
		return sb.toString();
	}

	public String DEBUG(String message) {

		StringBuilder sb = new StringBuilder();
		sb.append(dateFormat.format(System.currentTimeMillis()));
		sb.append(" DEBUG " + className + ": " + message + "\n");
		return sb.toString();
	}

	public String ERROR(String message) {

		StringBuilder sb = new StringBuilder();
		sb.append(dateFormat.format(System.currentTimeMillis()));
		sb.append(" ERROR " + className + ": " + message + "\n");
		return sb.toString();
	}

}