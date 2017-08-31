package com.zdp.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.itc.zeas.project.model.ProjectEntity;

public class ZDPDataLineage {

	private JsonNodeFactory factory;
	private ObjectNode parrentNode;
	private ObjectNode execution;
	private ArrayNode nodeArray;
	private ArrayNode connectionArray;
	private Set<Node> graphNodeSet;
	private Set<Connections> connectionSet;

	public ZDPDataLineage() {
		factory = JsonNodeFactory.instance;
		parrentNode = factory.objectNode();
		execution = factory.objectNode();
		nodeArray = factory.arrayNode();
		connectionArray = factory.arrayNode();
		graphNodeSet = new HashSet<Node>();
		connectionSet = new HashSet<Connections>();
	}

	public ProjectEntity getJson(String projectName, List<ProjectEntity> projectEntityList, String dataSetname) throws Exception {
		for (ProjectEntity projectEntity : projectEntityList) {
			if (projectEntity.getName().equalsIgnoreCase(projectName)) {
				ObjectMapper mapper = new ObjectMapper();
				JsonNode projectNode = mapper.readTree(projectEntity.getJsonblob());
				parrentNode.put("name", projectNode.get("name").textValue() + "_lineage");
				parrentNode.put("description", projectNode.get("description"));
				System.out.println(projectNode);
				if (projectNode.get("ExecutionGraph").get("nodes") != null) {
					Iterator<JsonNode> nodesItr = projectNode.get("ExecutionGraph").get("nodes").iterator();
					Iterator<JsonNode> connections = projectNode.get("ExecutionGraph").get("connections").iterator();
					Map<String, JsonNode> nodeListMap = new HashMap<>();
					List<JsonNode> connectionNodeLists = new ArrayList<>();
					List<JsonNode> connectionNodeListTarget = new ArrayList<>();
					Map<String, List<JsonNode>> sourceIdMap = new HashMap<>();
					Map<String, List<JsonNode>> targetIdMap = new HashMap<>();
					Set<Node> parrentNodeSet = new HashSet<>();
					Set<Connections> connectionSetgraph = new HashSet<>();
					while (nodesItr.hasNext()) {
						JsonNode nodeNode = nodesItr.next();
						nodeListMap.put(nodeNode.get("blockId").textValue(), nodeNode);
						parrentNodeSet.add(new Node(nodeNode.get("blockId").textValue(), nodeNode));
					}
					while (connections.hasNext()) {
						JsonNode connection = connections.next();
						String sourceId = connection.get("sourceId").textValue();
						if (sourceIdMap.containsKey(sourceId)) {
							sourceIdMap.get(sourceId).add(connection);
						} else {
							connectionNodeLists = new ArrayList<>();
							connectionNodeLists.add(connection);
						}
						sourceIdMap.put(sourceId, connectionNodeLists);
						connectionSetgraph
								.add(new Connections(connection.get("connectionId").textValue(), connection));
						String targetId = connection.get("targetId").textValue();
						if (targetIdMap.containsKey(targetId)) {
							targetIdMap.get(targetId).add(connection);
						} else {
							connectionNodeListTarget = new ArrayList<>();
							connectionNodeListTarget.add(connection);
						}
						targetIdMap.put(targetId, connectionNodeListTarget);
						connectionSetgraph
								.add(new Connections(connection.get("connectionId").textValue(), connection));
					}
					Node pNode = null;
					for (Node node : parrentNodeSet) {
						System.out.println(node.getJson().get("name"));
						if (node.getJson().get("name").textValue().equalsIgnoreCase(dataSetname)) {
							pNode = node;
						}
						if (sourceIdMap.containsKey(node.getBlockId())) {
							List<Node> childList = new ArrayList<>();
							for (JsonNode srcNode : sourceIdMap.get(node.getBlockId())) {
								String block = srcNode.get("targetId").textValue();
								childList.add(addChild(parrentNodeSet, new Node(block, nodeListMap.get(block))));
							}
							node.setChildList(childList);
						}
						if (targetIdMap.containsKey(node.getBlockId())) {
							List<Node> parrentList = new ArrayList<>();
							for (JsonNode tarNode : targetIdMap.get(node.getBlockId())) {
								String block = tarNode.get("sourceId").textValue();
								parrentList.add(addChild(parrentNodeSet, new Node(block, nodeListMap.get(block))));
							}
							node.setParrentList(parrentList);
						}
					}
					boolean isParrent = true;
					if (pNode.getParrentList() == null) {
						isParrent = false;
					}
					addToNode(pNode, sourceIdMap, targetIdMap, isParrent);
					if (pNode.getChildList() != null) {
						ListIterator<Node> itr = pNode.getChildList().listIterator();
						while (itr.hasNext()) {
							Node n = itr.next();
							addToNode(n, sourceIdMap, targetIdMap, isParrent);
							if (n.getChildList() != null) {
								for (Node nn : n.getChildList()) {
									itr.add(nn);
								}
							}
							if (isParrent) {
								if (n.getParrentList() != null) {
									for (Node nn : n.getParrentList()) {
										itr.add(nn);
									}
								}
							}
						}
					}
					int i = 0;
					for (Node n : graphNodeSet) {
						nodeArray.insert(i, n.getJson());
						i++;
					}
					i = 0;
					for (Connections n : connectionSet) {
						connectionArray.insert(i, n.getJson());
						i++;
					}
					execution.put("nodes", nodeArray);
					execution.put("connections", connectionArray);
				}
			}
			parrentNode.put("ExecutionGraph", execution);
		}

		ProjectEntity projectEntity = new ProjectEntity();
		projectEntity.setName(dataSetname + "_lineage");
		projectEntity.setJsonblob(parrentNode.toString());
		return projectEntity;
	}

	private Node addChild(Set<Node> parrentNodeSet, Node node) {
		for (Node nodes : parrentNodeSet) {
			if (nodes.equals(node)) {
				return nodes;
			}
		}
		return node;
	}

	private Boolean addToNode(Node node, Map<String, List<JsonNode>> sourceIdMap,
			Map<String, List<JsonNode>> targetIdMap, boolean isParrent) {
		Boolean status = false;
		if (graphNodeSet.add(node)) {
			status = true;
		}
		if (node.getChildList() != null) {
			for (Node nodes : node.getChildList()) {
				if (graphNodeSet.add(nodes)) {
					status = true;
					for (JsonNode src : sourceIdMap.get(node.getBlockId())) {
						connectionSet.add(new Connections(src.get("connectionId").textValue(), src));
					}
				}
			}
		}
		if (isParrent) {
			if (node.getParrentList() != null) {
				for (Node nodes : node.getParrentList()) {
					if (graphNodeSet.add(nodes)) {
						status = true;
						for (JsonNode src : sourceIdMap.get(node.getBlockId())) {
							connectionSet.add(new Connections(src.get("connectionId").textValue(), src));
						}
					}
				}
			}
		}
		return status;
	}

	public static void main(String[] args) throws Exception {
		ZDPDataLineage accessObject = new ZDPDataLineage();
		ZDPDataAccessObject dao = new ZDPDataAccessObjectImpl();
		List<ProjectEntity> projectEntityList = dao.findLatestVersionProjects("project");
		System.out.println(accessObject.getJson("2 train", projectEntityList, "Nisi_User_roles_dataset").getJsonblob());
	}

}

class Node {

	private String blockId;
	private List<Node> childList;
	private List<Node> parrentList;
	private JsonNode json;

	public List<Node> getChildList() {
		return childList;
	}

	public void setChildList(List<Node> childList) {
		this.childList = childList;
	}

	public List<Node> getParrentList() {
		return parrentList;
	}

	public void setParrentList(List<Node> parrentList) {
		this.parrentList = parrentList;
	}

	Node(String blockId, JsonNode json) {
		this.blockId = blockId;
		this.json = json;
	}

	public String getBlockId() {
		return blockId;
	}

	public void setBlockId(String blockId) {
		this.blockId = blockId;
	}

	public JsonNode getJson() {
		return json;
	}

	public void setJson(JsonNode json) {
		this.json = json;
	}

	@Override
	public boolean equals(Object obj) {

		// checking for null
		if (obj == null) {
			return false;
		}
		// checking for comparison
		if (this == obj) {
			return true;
		}
		Node ob = null;
		if (obj instanceof Node) {
			ob = (Node) obj;
			return this.blockId.equalsIgnoreCase(ob.blockId);
		}
		return false;

	}

	@Override
	public int hashCode() {
		int index = blockId.lastIndexOf("pipe");
		return Integer.parseInt(blockId.substring(index + 4, index + 8));
	}

	@Override
	public String toString() {
		return this.blockId;
	}
}

class Connections {
	String connectionId;
	JsonNode json;

	public String getConnectionId() {
		return connectionId;
	}

	public void setConnectionId(String connectionId) {
		this.connectionId = connectionId;
	}

	public JsonNode getJson() {
		return json;
	}

	public void setJson(JsonNode json) {
		this.json = json;
	}

	public Connections(String connectionId, JsonNode connection) {
		this.connectionId = connectionId;
		this.json = connection;
	}

	@Override
	public boolean equals(Object obj) {

		// checking for null
		if (obj == null) {
			return false;
		}
		// checking for comparison
		if (this == obj) {
			return true;
		}
		Connections ob = null;
		if (obj instanceof Connections) {
			ob = (Connections) obj;
			return this.connectionId.equalsIgnoreCase(ob.connectionId);
		}
		return false;

	}

	@Override
	public int hashCode() {
		int index = connectionId.lastIndexOf("_");
		return Integer.parseInt(connectionId.substring(index + 1, connectionId.length()));
	}
}
