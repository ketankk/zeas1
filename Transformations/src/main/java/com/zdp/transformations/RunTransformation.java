package com.zdp.transformations;

import com.zdp.transformations.factory.TransformationsFactory;
import com.zdp.transformations.iface.ITransformations;

public class RunTransformation {
	public static void main(String[] args) {
		TransformationsFactory tf = new TransformationsFactory();
		ITransformations it = tf.createTransformation(args);
		it.execute();
	}

}
