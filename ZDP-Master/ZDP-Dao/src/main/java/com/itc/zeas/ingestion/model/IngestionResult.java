package com.itc.zeas.ingestion.model;

import lombok.Data;

@Data
 public class IngestionResult {
	private String Date;
	private char Shift;
	private long  OffGasConsumption;
	private long totalOffGasFlare;
	private int CVOfTailGas;
	private double fOConsumptionDCS_Store;
	private int boilerSteamGeneration;
	private int turbineSteamConsumption;
	private String turbineVacuumMin_Max;

	private float condensateTemperature;
	private long totalPowerGeneration;
	private long stackTemp;
	private long economizerInletFeedwaterTemperature;
	private long gasQuantityTarget;
	private float exportTariff;
	private String remarks;
	private String gradeOfCBTread;
	private String gradeOfCBCarcess;
	private String oilRatioUsedForProduction;
	private float ProductionOfCB;
	private float specificGasGeneration;
		
}
