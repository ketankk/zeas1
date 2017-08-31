package com.itc.taphius.controller;


import java.util.List;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.itc.taphius.dao.EntityManager;
import com.itc.taphius.dao.StreamDriverManager;
import com.itc.taphius.model.Entity;
import com.itc.taphius.model.StreamDriver;
import com.itc.zeas.stream.StreamConsumerExecutor;


@RestController
@RequestMapping("/rest/service")
public class StreamConsumerController {
	
	
	/**
	 * @param Entity
	 * @return Entity
	 */
	@RequestMapping(value = "/addStreamEntity", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody
	StreamDriver addEntity(@RequestBody StreamDriver entity) {
		StreamDriverManager sdmgr=new StreamDriverManager();
		sdmgr.addEntity(entity);
		entity=sdmgr.getEntityByName(entity.getName());
		return entity;

	}
	
	
	
	
	@RequestMapping(value = "/Streaming", method = RequestMethod.GET, headers = "Accept=application/json")
	public @ResponseBody
	List<Entity> getStreamingEntityList() {
		StreamDriverManager sdmgr=new StreamDriverManager();
		List<Entity> streamingEntities = sdmgr.getStreamingEntity();
		return streamingEntities;
	}
	
	
	
	

	/**
	 * @param String
	 * @return String
	 */
	@RequestMapping(value = "/startStream", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody
	String runStreamConsumer(@RequestBody Entity entity) {
		System.out.println("Starting Stream consumer");
		StreamConsumerExecutor sce=new StreamConsumerExecutor();
		return sce.executeConsumer(entity);

	}
	

	/**
	 * 
	 */
	@RequestMapping(value = "/getStreamDrivers", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody
	List<StreamDriver> getDriver(@RequestBody Entity entity) {
		String name=entity.getName();
		System.out.println("******************* getting stream drivers ***************************");
		StreamDriverManager sdmgr=new StreamDriverManager();
		String status="RUNNING";
		
		List<StreamDriver> drivers=sdmgr.getStreamDriver(name,status);
		
		return drivers;

	}
	
	/**
	 * 
	 */
	@RequestMapping(value = "/getRunningJobs", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody
	List<StreamDriver> getRunningJobs(@RequestBody Entity entity) {
		System.out.println("******************* getting Running Jobs ***************************");
		StreamDriverManager sdmgr=new StreamDriverManager();
		String status="RUNNING";
		String name=null;
		List<StreamDriver> drivers=sdmgr.getStreamDriver(name,status);
		
		return drivers;

	}
	
	@RequestMapping(value = "/stopStreamDriver", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody
	void stopDriver(@RequestBody StreamDriver driver) {
		
		
		
		System.out.println("stopping  Streaming jobsdrivers");
		System.out.println(driver.getDriverId());
			
		StreamConsumerExecutor scexec=new StreamConsumerExecutor();
		scexec.stopConsumer(driver.getDriverId(), driver.getStopBy());
	

	}
	

}
