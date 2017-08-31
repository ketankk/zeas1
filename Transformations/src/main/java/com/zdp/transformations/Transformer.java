package com.zdp.transformations;

import com.zdp.transformations.impl.ReplaceOrRemoveMissingData;

public class Transformer {

	/*
	 * args[0] = the Type of Transformation args[1] = The schema of the data set
	 * where feilds separated by "," and the fields and it's data type are
	 * separated by ":" args[2] = <source Folder Location>:<destination Folder
	 * Location>:<subtype of the transformation>:<column indexes>:<value to be
	 * replaced as applicable>
	 */
	public static void main(String[] args) {

		String schema = "name:String,age:int,salary:int";
		String customval = "99";
		String indexes = "1,2";
		String type = "Remove entire row";
		ReplaceOrRemoveMissingData obj = new ReplaceOrRemoveMissingData(schema, customval, indexes,
				"hdfs://Zlab-physrv1:8020/user/nisith/data/TestEmp.txt", "hdfs://Zlab-physrv1:8020/user/nisith/remov/",
				type);
		obj.execute();
	}
}
