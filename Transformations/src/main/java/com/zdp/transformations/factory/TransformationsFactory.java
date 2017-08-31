package com.zdp.transformations.factory;

import com.zdp.transformations.iface.ITransformations;
import com.zdp.transformations.impl.ColumnFilter;
import com.zdp.transformations.impl.Group;
import com.zdp.transformations.impl.Join;
import com.zdp.transformations.impl.Partition;
import com.zdp.transformations.impl.ReplaceOrRemoveMissingData;
import com.zdp.transformations.impl.Sort;
import com.zdp.transformations.impl.Subset;

public class TransformationsFactory {

	public ITransformations createTransformation(String[] args) {
		if (args[0].equalsIgnoreCase("Column_Filter")) {
			// logger.info(args);
			return new ColumnFilter(args);
		} else if (args[0].equalsIgnoreCase("Subset")) {
			return new Subset(args);
		} else if (args[0].equalsIgnoreCase("Partition")) {
			return new Partition(args);
		} else if (args[0].equalsIgnoreCase("Clean_Missing_Data")) {
			return new ReplaceOrRemoveMissingData(args);
		}
		 else if (args[0].equalsIgnoreCase("Join")) {
				return new Join(args);
			}
		 else if (args[0].equalsIgnoreCase("Sort")) {
				return new Sort(args);
			}
		 else if (args[0].equalsIgnoreCase("Group_by")) {
				return new Group(args);
			}
		
		
		return null;
	}

}
