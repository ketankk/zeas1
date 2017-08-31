package com.itc.zeas.datagovernance;

import java.util.List;

import javax.ws.rs.QueryParam;

import org.apache.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasException;

/**
 * @author Ketan on 5/27/2017.
 */
@RestController
@RequestMapping("/rest/service/governance")
public class LineageController {

	private static final Logger LOG = Logger.getLogger(LineageController.class);

	LineageService lineageService;

	/**
	 * api to get list of entity based on type
	 *
	 * @param type
	 * @return
	 */
	@RequestMapping(value = "/listentity", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<?> getListofHiveTables(@RequestParam(value = "type") String type) {
		List<AtlasEntity> entityList;
		lineageService = new LineageService();
		try {
			entityList = lineageService.listEntity();
			return ResponseEntity.ok(entityList);
		} catch (ZeasException e) {
			LOG.error("Exception while getting list of hive tables " + e.getMessage());
			return ResponseEntity.status(e.getErrorCode())
					.body("Exception while getting list of hive tables: " + e.getMessage());
		} catch (Exception e) {

			LOG.error("Exception while getting list of hive tables :" + e.getMessage());

			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
	}

	/**
	 * Api to get Lineage grapgh based on guid of entity
	 *
	 * @param guid
	 * @return
	 */
	@RequestMapping(value = "/lineage/{guid}", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<?> getLineageForEntity(@PathVariable("guid") String guid) {
		try {

			lineageService = new LineageService();

			HiveLineage hiveLineage = lineageService.getLineage(guid);
			return ResponseEntity.ok(hiveLineage);
		} catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
	}

	/**
	 * Api to get types of entities in atlas it returns list of type's name
	 *
	 * @return
	 */
	@RequestMapping(value = "/types", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<?> getAtlasEntityTypes(@QueryParam("type") String type) {
		lineageService = new LineageService();
		try {
			List<String> typeList = lineageService.getTypes(type);
			LOG.info("Types of Atlas entities are " + typeList);
			return ResponseEntity.ok(typeList);
		} catch (ZeasException e) {
			LOG.error(e.getMessage());
			return ResponseEntity.status(e.getErrorCode()).body(e.getMessage());

		} catch (Exception e) {
			LOG.error("Exception occurred while getting list of types");
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
	}

	/**
	 * api to create any new tag
	 * 
	 * @param string
	 * @return
	 */
	@RequestMapping(value = "/types", method = RequestMethod.POST, headers = "Accept=application/json")
	public ResponseEntity<?> createTag(@RequestBody String string) {
		LineageService lineage = new LineageService();
		try {
			int status = lineage.createTag(string);
			return ResponseEntity.ok(status);
		} catch (ZeasException e) {
			return ResponseEntity.status(e.getErrorCode()).body(e.getMessage());
		} catch (Exception e) {
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}

	}

	/**
	 * api to assign any tag to entity with guid
	 * 
	 * @param data
	 * @param guid
	 * @return
	 */

	@RequestMapping(value = "/entities/{guid}/tag", method = RequestMethod.POST, headers = "Accept=application/json")
	public ResponseEntity<?> assignTagToEntity(@RequestBody String data, @PathVariable("guid") String guid) {

		LineageService lineage = new LineageService();
		try {
			lineage.assignTag(guid, data);
			List<AtlasEntity> updatedTableList = lineage.listEntity();
			return ResponseEntity.ok(updatedTableList);
		} catch (ZeasException e) {
			return ResponseEntity.status(e.getErrorCode()).body(e.getMessage());
		} catch (Exception e) {
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}

	}

	@RequestMapping(value = "/entities/textsearch", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<?> listEntityByText(@QueryParam("query") String query) {
		LineageService lineage = new LineageService();
		try {
			return ResponseEntity.ok(lineage.textSearchResult(query));
		} catch (ZeasException e) {
			LOG.error("Exception in getting entity list by tag name: " + query + " Exception is " + e.getMessage());
			return ResponseEntity.status(e.getErrorCode()).body(e.getMessage());
		} catch (Exception e) {
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}

	}

	/**
	 * returns list of entities based on tag name
	 * 
	 * @param tagName
	 * @return
	 */

	@RequestMapping(value = "/entities", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<?> listEntityByTag(@QueryParam("tagName") String tagName) {
		LineageService lineage = new LineageService();
		try {
			return ResponseEntity.ok(lineage.getEntitiesByTag(tagName));
		} catch (ZeasException e) {
			LOG.error("Exception in getting entity list by tag name: " + tagName + " Exception is " + e.getMessage());
			return ResponseEntity.status(e.getErrorCode()).body(e.getMessage());
		} catch (Exception e) {
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}

	}

	/**
	 * de-assign a tag from entity with guid
	 * 
	 * @param guid
	 * @param tagName
	 * @return
	 */
	@RequestMapping(value = "/entities/{guid}/traits/{tagName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
	public @ResponseBody ResponseEntity<?> removeTagFromEntity(@PathVariable("guid") String guid,
			@PathVariable("tagName") String tagName) {
		List<AtlasEntity> entityList;
		lineageService = new LineageService();
		LineageService lineage = new LineageService();
		try {
			lineage.removeTag(guid, tagName);
			entityList = lineageService.listEntity();
			return ResponseEntity.ok(entityList);
		} catch (ZeasException e) {
			return ResponseEntity.status(e.getErrorCode()).body(e.getMessage());
		} catch (Exception e) {
			return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}

	}

	public static void main(String[] args) {
		String data = "{\"jsonClass\":\"org.apache.atlas.typesystem.json.InstanceSerialization$_Struct\",\"typeName\":\"test13\",\"values\":{\"HdfsAccess\":\"qwe\",\"HIveAccess\":\"asd\"}}\n";
		String guid = "261ed1ac-0e5a-472a-b18f-a4a3773b8e54";
		String guid2 = "aa19f214-2323-494d-8167-286709753b57";
		// System.out.println(new LineageController().addTagToEntity(data,
		// guid));
		System.out.println("bn \\\\  \t \n \f \b nb".trim());
		System.out.println(System.lineSeparator());
		//System.out.println(new LineageController().getLineageForEntity(guid2));

	}
}
