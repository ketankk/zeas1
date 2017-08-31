package com.itc.zeas.datagovernance;

import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.itc.zeas.exceptions.ZeasException;

/**
 * @author Ketan on 5/30/2017.
 */
public class LineageService {

	Logger LOG = Logger.getLogger(LineageService.class);
	private AtlasHelper atlasHelper = new AtlasHelper();

	public List<AtlasEntity> listEntity() throws Exception {
		// Map of GUID and table name

		return atlasHelper.getHiveDbTableListMap();

	}

	public HiveLineage getLineage(String guid) throws Exception {

		HiveLineage hiveLineage = new HiveLineage();

		hiveLineage.setEntityGuid(guid);
		JsonElement inputGraphData = atlasHelper.getLineageInput(guid);
		JsonElement outputGraphData = atlasHelper.getLineageOutput(guid);

		JsonElement inEdges = inputGraphData.getAsJsonObject().get("edges");
		JsonElement outEdges = outputGraphData.getAsJsonObject().get("edges");

		List<EdgeData> atlasEdges = getEdges(inEdges, outEdges);
		Set<NodeData> nodeSet = constructNode(guid, inEdges, outEdges);

		hiveLineage.setEdges(atlasEdges);
		hiveLineage.setNodes(nodeSet);
		String propJson = atlasHelper.getEntityDetails(guid).toString();//.getAsString();
		ObjectMapper mapper = new ObjectMapper();
		JsonNode entityDet = mapper.readValue(propJson, JsonNode.class);

		hiveLineage.setPropJson(entityDet);
		JsonNode entityAudit = atlasHelper.getEntityAudit(guid);
		hiveLineage.setAuditJson(entityAudit);
		JsonNode schemaJson = atlasHelper.getSchemaJson(guid);
		hiveLineage.setSchemaJson(schemaJson);

		return hiveLineage;

	}

	/**
	 * method to set name type and query to set of nodes
	 *
	 * @param inEdges
	 * @param outEdges
	 * @return
	 * @throws MalformedURLException
	 */
	private Set<NodeData> constructNode(String targetGuid, JsonElement inEdges, JsonElement outEdges)
			throws ZeasException, MalformedURLException {
		Set<String> uniqueNodes = getUniqueNodes(inEdges, outEdges);
		Set<NodeData> nodeSet = new HashSet<>();
		Iterator<String> iter = uniqueNodes.iterator();
		while (iter.hasNext()) {
			String nodeId = iter.next();
			JsonElement json = atlasHelper.getEntityDetails(nodeId);
			String typeName = json.getAsJsonObject().get("typeName").getAsString();
			String name = json.getAsJsonObject().get("values").getAsJsonObject().get("name").getAsString();
			JsonElement qryTxt = json.getAsJsonObject().get("values").getAsJsonObject().get("queryText");
			String query = null;
			boolean isTarget = false;
			if (nodeId.equals(targetGuid))
				isTarget = true;
			if (qryTxt != null)
				query = qryTxt.getAsString();
			Node node = new Node(nodeId, name, typeName, query, isTarget);
			NodeData nodeData = new NodeData(node);
			nodeSet.add(nodeData);

		}
		return nodeSet;
	}

	/**
	 * method to create list of entity(nodes), which contains node guid and its
	 * taget nodes ids target is to create list of nodes with it's output nodes
	 *
	 * @param inEdges
	 * @param outEdges
	 * @return
	 */
	private Set<String> getUniqueNodes(JsonElement inEdges, JsonElement outEdges) {

		Set<String> nodeSet = new HashSet<>();
		// create list of nodes with out
		for (Map.Entry<String, JsonElement> ele : outEdges.getAsJsonObject().entrySet()) {
			String sourceGuid = ele.getKey();
			nodeSet.add(sourceGuid);
			Set<String> vals = convertTolist(ele.getValue().toString());
			for (String val : vals) {
				nodeSet.add(val);
			}

		}
		for (Map.Entry<String, JsonElement> ele : inEdges.getAsJsonObject().entrySet()) {
			String sourceGuid = ele.getKey();
			nodeSet.add(sourceGuid);
			Set<String> vals = convertTolist(ele.getValue().toString());
			for (String val : vals) {

				nodeSet.add(val);
			}

		}
		return nodeSet;
	}

	private List<EdgeData> getEdges(JsonElement inEdges, JsonElement outEdges) {

		List<EdgeData> atlasEdges = new ArrayList<>();

		for (Map.Entry<String, JsonElement> ele : outEdges.getAsJsonObject().entrySet()) {

			String key = ele.getKey();
			Set<String> vals = convertTolist(ele.getValue().toString());

			for (String val : vals) {
				AtlasEdge atlasEdge = new AtlasEdge();
				atlasEdge.setId(key + "_" + val);
				atlasEdge.setSource(key);
				atlasEdge.setTarget(val);

				EdgeData edgeData = new EdgeData();
				edgeData.setData(atlasEdge);
				atlasEdges.add(edgeData);
			}
		}
		// if node already exists then get that node add
		for (Map.Entry<String, JsonElement> ele : inEdges.getAsJsonObject().entrySet()) {

			String key = ele.getKey();
			Set<String> vals = convertTolist(ele.getValue().toString());
			for (String val : vals) {
				AtlasEdge atlasEdge = new AtlasEdge();
				atlasEdge.setId(val + "_" + key);
				// if its inedge then key is target and values are source
				atlasEdge.setSource(val);
				atlasEdge.setTarget(key);

				EdgeData edgeData = new EdgeData();
				edgeData.setData(atlasEdge);
				atlasEdges.add(edgeData);
			}
		}

		return atlasEdges;
	}

	public int createTag(String json) throws ZeasException {

		return atlasHelper.createTag(json);

	}

	private Set<String> convertTolist(String val) {
		Type listType = new TypeToken<Set<String>>() {
		}.getType();
		Set<String> list = new Gson().fromJson(val, listType);
		return list;
	}

	/**
	 * method to assign tag to any entity
	 * 
	 * @param guid
	 * @param data
	 * @return
	 * @throws ZeasException
	 */
	public int assignTag(String guid, String data) throws ZeasException {
		AtlasHelper atlasHelper = new AtlasHelper();
		return atlasHelper.assignTag(guid, data);
	}

	/**
	 * method takes the tag name and gets the guid and filters those entities
	 * related to that tag
	 * 
	 * @return
	 * 
	 * @throws Exception
	 */
	List<AtlasEntity> allEntity;
	List<String> guids;

	public List<AtlasEntity> getEntitiesByTag(String tagName) throws Exception {

		// get whole list of hive tables from zeas db
		allEntity = listEntity();
		guids = atlasHelper.getGuidbyTag(tagName);

		List<AtlasEntity> filteredEntity = new ArrayList<>();

		for (AtlasEntity entity : allEntity) {
			if (guids.contains(entity.getEntityGuid())) {
				filteredEntity.add(entity);
			}
		}
		return filteredEntity;

	}

	public int removeTag(String guid, String tagName) throws ZeasException {
		AtlasHelper atlasHelper = new AtlasHelper();
		return atlasHelper.removeTag(guid, tagName);
	}

	public List<AtlasEntity> textSearchResult(String query) throws ZeasException {
		AtlasHelper atlasHelper = new AtlasHelper();
		List<String> res = atlasHelper.textSearch(query);
		List<AtlasEntity> atlasEntities = new ArrayList<>();
		for (String guid : res) {
			JsonElement json = atlasHelper.getEntityDetails(guid);
			AtlasEntity atlasEntity = atlasHelper.constructEntity(json);
			atlasEntities.add(atlasEntity);
		}
		return atlasEntities;
	}

	public static void main(String[] args) throws Exception {
		String guid = "05dec44b-c4c4-44ea-9564-7acd8c0cf015";// 3137'
		String guid2 = "c4dd08ee-0deb-4fc9-b9f3-6cc6a05701e0";// 3212\

		// System.out.println(new LineageService().getLineage(guid));
		System.out.println("filteredEntity " + new LineageService().textSearchResult("m_3137_1"));

	}

	public List<String> getTypes(String type) throws ZeasException, MalformedURLException {

		AtlasHelper atlasHelper = new AtlasHelper();
		return atlasHelper.getTypeList(type);
	}

}
