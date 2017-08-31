package com.itc.zeas.ingestion;


import java.sql.SQLException;
import java.util.List;


import org.json.simple.JSONObject;
import org.springframework.web.bind.annotation.PathVariable;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import com.itc.zeas.project.QueryManager;


/**
 * @author 11786
 * 
 */
@RestController
@RequestMapping("/rest/service")
public class QueryController {
 

           
 /**
 * This method is used to get the dynamic select query result 
 * @return List<JSONObject>
 * @throws SQLException 
 */
	@RequestMapping(value="/query/{sQuery}", method = RequestMethod.GET,headers="Accept=application/json")

	 public List<JSONObject> listDataSchema(@PathVariable("sQuery") String sQuery) throws SQLException {
		QueryManager queryManager =  new QueryManager();
		List<JSONObject> queryResult = queryManager.getResult(sQuery);
	    return queryResult;
	 }
 
 }
