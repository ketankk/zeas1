package com.zdp.transformations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestIt {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String schema = "name:String,age:int,salary:int";
		final String customval = "99";
		String indexes = "2,1";
		Map<String, String> delimiters = new HashMap();
		delimiters.put("tab", "\t");
		delimiters.put("space", "\\s+");
		delimiters.put("comma", ",");
		delimiters.put("underscore", "_");
		delimiters.put("slash", "-");
		delimiters.put("Control-A", "\\^A");
		delimiters.put("Control-B", "\\^B");
		delimiters.put("Control-C", "\\^C");
		delimiters.put("newline", "\n");
		delimiters.put("carriage return", "\r\n");
		
		final String[] indexArr = indexes.split(",");
		String tmp = "";
		String arg0 = "Asim,,340";
		String[] val = arg0.split(delimiters.get("comma"),-1);
		for (String in : indexArr) {
			int i = new Integer(in);
			if (null == val[i] || val[i].isEmpty()) {
System.out.println("in");
			} else {
				System.out.println("out");
			}
		}
		Double d=8.4;
		Long l = 4l;
		System.out.println(d/l);
		List<String> s = Arrays.asList(indexArr);
		List<String> c =new ArrayList<String>();
		c.add("55");
		c.add("66");
		Map<String,String> map = new HashMap<String,String>();

		for(int i=0;i<s.size();i++){
			System.out.println("ind : "+s.get(i));
			System.out.println("val : "+c.get(i));
			map.put(s.get(i), c.get(i));
		}
		for(int i=0;i<s.size();i++){
			
			System.out.println("map : "+map.get(s.get(i)));
			int[] partition = new int[]{i};
			System.out.println("ASrray : "+partition[0]);
		}	
		System.out.println("Long val"+new Long(new Double(Math.round(new Double("234.5"))).longValue()).toString());

		
		//String arg0 = "00 US United States              239713822   39803537     492720 14.2  0.2   63696617    8617432     244607 11.9  0.3 SAHIE States 11JUL2005";
/*		String arg0 = "1";
		String[] val = arg0.split(delimiters.get("comma"),-1);
		System.out.println(" space :"+delimiters.get("space"));
		System.out.println(Arrays.asList(val));
		System.out.println(val.length);*/
/*		for (Integer i = 0; i < val.length; i++) {

			if ((null == val[i] || val[i].isEmpty()) && Arrays.asList(indexArr).contains(i.toString())) {
				tmp = tmp + "," + customval;
			} else {
				tmp = tmp + "," + val[i];
			}
		}
		System.out.println("tmp " + tmp);
		StringTokenizer str = new StringTokenizer(arg0, ",");

		while (str.hasMoreTokens()) {
			System.out.println("Tokenizer : " + str.nextToken());
		}*/

	}

}
