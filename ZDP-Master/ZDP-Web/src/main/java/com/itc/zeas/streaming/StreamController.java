package com.itc.zeas.streaming;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.config.YamlProcessor.ResolutionMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.exceptions.ZeasSQLException;
import com.itc.zeas.profile.model.Entity;
import com.itc.zeas.project.model.NameAndDataType;
import com.itc.zeas.streaming.daoimpl.StreamDriverManager;
import com.itc.zeas.streaming.model.KafkaDetails;
import com.itc.zeas.streaming.model.KafkaTopic;
import com.itc.zeas.streaming.model.StreamingEntity;
import com.itc.zeas.utility.FileUtility;
import com.itc.zeas.utility.utils.CommonUtils;

/**
 * @author ketan Apr 25, 2017
 *         <p>
 *         This controller is used for api calls related to Streaming UI js file
 *         streamctrl.js uses it mostly
 */
@RestController
@RequestMapping("/rest/service")
public class StreamController {
	private static final Logger LOG = Logger.getLogger(StreamController.class);

	/**
	 * This api is invoked when user is creating any consumer or producer
	 *
	 * @return Entity
	 * @throws IOException
	 */
	@RequestMapping(value = "/addStreamEntity", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<?> addEntity(@RequestBody StreamingEntity entity)
			throws IOException {

		LOG.info("/addStreamEntity");
		StreamDriverManager sdmgr = new StreamDriverManager();
		try {
			sdmgr.addEntity(entity);
			entity = sdmgr.getEntityByName(entity.getName());
		} catch (ZeasSQLException e) {
			LOG.error(e.getMessage());
			return ResponseEntity.status(e.getErrorCode()).body(e.getMessage());

		} catch (Exception e) {
			LOG.error(e.getMessage());
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());

		}
		LOG.info("New Streaminmg entity added successfully " + entity);
		return ResponseEntity.ok(entity);

	}

	/**
	 * api to create topic in kafka broker
	 *
	 * @param topic
	 * @return
	 * @throws IOException
	 * @code KafkaTopic topic contains topic name, partition and replication
	 *       factor
	 */

	@RequestMapping(value = "/createKafkaTopic", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<String> createTopic(@RequestBody KafkaTopic topic)
			throws IOException {
		try {
			KafkaHelper helper = new KafkaHelper();
			topic = helper.createTopic(topic);
		} catch (ZeasException e) {
			LOG.error("Exception while creating Topic " + topic.toString() + " " + e.getMessage());

			return ResponseEntity.status(e.getErrorCode()).body(e.getMessage());
		} catch (Exception e) {
			LOG.error("Exception while creating Topic " + topic.toString() + " " + e.getMessage());

			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
		return ResponseEntity.ok("Topic created Successfully " + topic);

	}

	/**
	 * Delete entities as well as clean the data set from hive related to
	 * streaming.
	 *
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "/deleteEntity/{entityName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<?> deleteEntity(@PathVariable("entityName") String entityName) throws IOException {
		StreamDriverManager sdmgr = new StreamDriverManager();
		LOG.info("Deleting Streaming entity " + entityName);
		try {
			return ResponseEntity.ok(sdmgr.deleteStreamingEntity(entityName));
		} catch (ZeasException e) {
			LOG.info("Exception occurred while deleting Streaming entity " + entityName);
			return ResponseEntity.status(e.getErrorCode()).body(e.getMessage());
		} catch (Exception e) {
			LOG.info("Exception occurred while deleting Streaming entity " + entityName);
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());

		}

	}

	@RequestMapping(value = "/Streaming", method = RequestMethod.GET, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<?> getStreamingEntityList(HttpServletRequest httpServletRequest)   {
		if (httpServletRequest == null) {

			LOG.error("Not Authorized for this action");
			return ResponseEntity.status(ZeasErrorCode.SC_UNAUTHORIZED).body("Not Authorized");
		}
		List<StreamingEntity> streamingEntities = null;
		try {
			StreamDriverManager sdmgr = new StreamDriverManager();
			streamingEntities = sdmgr.getStreamingEntity(httpServletRequest);
		} catch (ZeasException e) {
			LOG.error("Exception while getting list of streams " + e.getMessage());
			return ResponseEntity.status(e.getErrorCode()).body(e.getMessage());

		} catch (Exception e) {
			LOG.error("Exception while getting list of streams " + e.getMessage());
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());

		}
		return ResponseEntity.ok(streamingEntities);
	}

	/**
	 * Start the streaming process.
	 *
	 * @return String
	 * @throws IOException
	 */
	
	@RequestMapping(value = "/startStream", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<?> runStreamConsumer(@RequestBody StreamingEntity entity) {
		try {
			String type = entity.getType();
			StreamExecutorController controller = new StreamExecutorController(type);
			return ResponseEntity.ok(controller.startStream(entity));
		} catch (ZeasException e) {
			LOG.error("Couldn't start Stream due to Exception " + e.getMessage());
			return ResponseEntity.status(e.getErrorCode()).body(e.getMessage());
		} catch (Exception e) {
			LOG.error("Couldn't start Stream due to Exception " + e.getMessage());
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}

	}

	@RequestMapping(value = "/uploadSchemaFile", method = RequestMethod.POST)
	public @ResponseBody ResponseEntity<?> uploadSchemaFile(@RequestParam("file") MultipartFile schemaFile) {
		File convFile = new File(schemaFile.getOriginalFilename());
		List<NameAndDataType> schema = FileUtility.getSchema(convFile);
		try {
			schemaFile.transferTo(convFile);
		} catch (Exception e) {
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body("Exception " + e.getMessage());
		}
		return ResponseEntity.ok(schema);
	}
/**
 * get details for entity for updation based on entity id
 * @param id
 * @return
 */
	@RequestMapping(value = "/update/{id}", method = RequestMethod.GET)
	public ResponseEntity<?> getEntityById(@PathVariable String id) {
		StreamDriverManager manager = new StreamDriverManager();
		LOG.info("Getting StreamingEntity for entity id:" + id);
		StreamingEntity entity = null;
		try {
			entity = manager.getEntityById(id);
			LOG.info("StreamingEntity for entity id:\n" + entity);

		} catch (ZeasException e) {
			LOG.error("Exception while updating Streaming entity with id '" + id + "' " + e.getMessage());
			return ResponseEntity.status(e.getErrorCode()).body(e.getMessage());
		} catch (Exception e) {
			LOG.error("Exception while updating Streaming entity with id '" + id + "' " + e.getMessage());
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}

		return ResponseEntity.ok(entity);

	}
/**
 * update entity
 * @param entity
 * @return
 */
	@RequestMapping(value = "/update", method = RequestMethod.POST, headers = "Accept=application/json")
	public ResponseEntity<?> updateEntity(@RequestBody StreamingEntity entity) {
		StreamDriverManager manager = new StreamDriverManager();
		try {
			manager.updateEntity(entity);
		} 
		catch(ZeasException e){
			LOG.error("Exception happened while updating entity " + entity.getName());
			return ResponseEntity.status(e.getErrorCode()).body(e.getMessage());
		}
		catch (Exception e) {
			LOG.error("Exception happened while updating entity " + entity.getName());
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
		return ResponseEntity.ok(entity);

	}

	@RequestMapping(value = "/kafkadetails", method = RequestMethod.GET, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<?> getKafkaDetail() {
		KafkaDetails kafkaDetails = null;
		try {
			KafkaHelper helper = new KafkaHelper();
			kafkaDetails = helper.kafkaDetails();
		} catch (ZeasException e) {
			LOG.error("Exception occurred while getting kafkadetails " + e.getMessage());
			return ResponseEntity.status(e.getErrorCode()).body(e.getMessage());
		} catch (Exception e) {
			LOG.error("Exception occurred while getting kafkadetails " + e.getMessage());
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
		return ResponseEntity.ok(kafkaDetails);

	}

	/**
	 * a method which return list of topic for a given zookeeper host port
	 *
	 * @param host
	 * @param port
	 * @return
	 */

	@RequestMapping(value = "/kafkatopics", method = RequestMethod.GET, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<?> getKafkaTopics(@RequestParam String host, String port) {
		List<KafkaTopic> topics = null;
		try {
			KafkaHelper helper = new KafkaHelper();
			topics = helper.getKafkaTopics(host, Integer.parseInt(port));
			return ResponseEntity.ok(topics);
		} catch (Exception e) {
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());

		}

	}

	/**
	 * This API will create new rule and Start transformation consumer.
	 *
	 * @return String {name:'name',location:'location',topic:'topic'}
	 * @throws IOException
	 */

	// change this to create rule
	@RequestMapping(value = "/createRule", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<?> createNewRule(@RequestBody StreamingEntity entity) {
		boolean is = false;
		entity.setType("transformation");
		StreamTransExecutor ste = new StreamTransExecutor();

		try {
			is = ste.addRule(entity);

		} catch (ZeasException e) {
			LOG.error("Couldn't add Transormation rule due to Exception " + e.getMessage());
			return ResponseEntity.status(e.getErrorCode()).body(e.getMessage());

		} catch (Exception e) {
			LOG.error("Couldn't add Transormation rule due to Exception " + e.getMessage());
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
		return ResponseEntity.ok(is);

	}
	

	/**
	 * This method will return list of transformation created for streaming data
	 *
	 * @return
	 */
	@RequestMapping(value = "/getTransformationRulesName", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<?> getTransformationRuleList() {
		List<String> rulesName = null;
		try {
			TransformationRule ruleObject = new TransformationRule();
			rulesName = ruleObject.getRulesName();
			return  ResponseEntity.ok(rulesName);

		} catch (Exception e) {
			return  ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
		// return responseEntity;

	}

	/**
	 * @throws IOException
	 * @throws SQLException
	 */
	@RequestMapping(value = "/getStreamDrivers", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<Object> getDriver(@RequestBody Entity entity,
			HttpServletRequest httpServletRequest ) {
		ResponseEntity<Object> drivers = null;
		try {
			CommonUtils commonUtils = new CommonUtils();
			String userName = commonUtils.extractUserNameFromRequest(httpServletRequest);
			String name = entity.getName();
			StreamTransExecutor ste = new StreamTransExecutor();
			drivers = ste.getStreamDriver(name, userName);
		} catch (Exception e) {
			

		}
		return drivers;

	}

	/**
	 * @param response
	 * @throws IOException
	 * @throws SQLException
	 */
	@RequestMapping(value = "/getRunningJobs", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<Object> getRunningJobs(@RequestBody Entity entity,
			HttpServletRequest httpServletRequest, ServletResponse response) {
		ResponseEntity<Object> drivers = null;
		try {
			CommonUtils commonUtils = new CommonUtils();
			String userName = commonUtils.extractUserNameFromRequest(httpServletRequest);
			StreamTransExecutor ste = new StreamTransExecutor();
			String name = null;
			drivers = ste.getStreamDriver(name, userName);
		} catch (Exception e) {
			try {
				response.getWriter().print(e.getMessage());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

		}
		return drivers;

	}

	@RequestMapping(value = "/stopStreamDriver", method = RequestMethod.POST, headers = "Accept=application/json")
	public @ResponseBody void stopDriver(@RequestBody StreamingEntity driver) {

		System.out.println("stopping  Streaming jobsdrivers");

		StreamConsumerExecutor scexec = new StreamConsumerExecutor();
		scexec.stopConsumer(driver.getName(), String.valueOf(driver.getId()), driver.getStopBy());
	}
}
