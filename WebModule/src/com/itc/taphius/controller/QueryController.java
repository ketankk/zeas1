package com.itc.taphius.controller;


import java.util.List;


import org.json.simple.JSONObject;
import org.springframework.web.bind.annotation.PathVariable;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import com.itc.taphius.dao.QueryManager;


/**
 * @author 11786
 * 
 */
@RestController
@RequestMapping("/rest/service")
public class QueryController {
 

           
 /**
 * This method is used to get the dynamic select query result 
 * @param String
 * @return List<JSONObject>
 */
	@RequestMapping(value="/query/{sQuery}", method = RequestMethod.GET,headers="Accept=application/json")

	 public List<JSONObject> listDataSchema(@PathVariable("sQuery") String sQuery) {
		QueryManager queryManager =  new QueryManager();
		List<JSONObject> queryResult = queryManager.getResult(sQuery);
	    return queryResult;
	 }
 
 }
