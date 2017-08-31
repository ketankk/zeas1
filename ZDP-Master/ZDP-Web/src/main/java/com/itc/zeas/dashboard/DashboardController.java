package com.itc.zeas.dashboard;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import com.itc.zeas.dashboard.daoimpl.DashBordServiceDB;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.itc.zeas.utility.utils.CommonUtils;
import com.itc.zeas.dashboard.dashboard.DashboardService;

@RestController
@RequestMapping("/rest/service/dashboard")
public class DashboardController {

	@RequestMapping(value = "/CallGraylogRestApi/{searchType}", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<Object> CallGraylogRestApi(
			@PathVariable("searchType") String searchType,
			HttpServletRequest httpServletRequest) throws IOException {

		DashboardService dashService = new DashboardService();

		return dashService.getDashboardDataFromGraylog(searchType, httpServletRequest);
	}
	
	@RequestMapping(value = "/getDashboardDetails/{searchType}/{graphType}", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<Object> getDashboardDetails(@PathVariable("searchType") String searchType,
			@PathVariable("graphType") String graphType, HttpServletRequest httpServletRequest) {
		
		DashBordServiceDB dashboardService = new DashBordServiceDB();
		CommonUtils commonUtils = new CommonUtils();
		String userName = commonUtils.extractUserNameFromRequest(httpServletRequest);
		return dashboardService.getDashboardDataFromDB(searchType, graphType,userName,"weekly");
	}
}
