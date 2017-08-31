package com.zdp.transformations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataFrameCreation {
	public static void main(String[] args) {
		String s = "subbu,hari,teja,naren";
		List<String> myList = new ArrayList<String>(Arrays.asList(s.split(",")));
		System.out.println(myList.get(0));
		System.out.println(myList.get(1));
	}

}
