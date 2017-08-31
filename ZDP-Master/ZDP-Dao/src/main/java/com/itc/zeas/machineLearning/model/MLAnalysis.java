package com.itc.zeas.machineLearning.model;

import com.itc.zeas.model.ProcessedPipeline;
import lombok.Data;

import java.util.List;

@Data
public class MLAnalysis {
	
	private int mlId;
	private ProcessedPipeline training;
	private ProcessedPipeline testing;
	private String algorithm;
	private int accuracy;
	private String size;
	private String features;
	private String label;
	private List<String> schema;

}
