package com.itc.zeas.datagovernance;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

import java.util.List;
import java.util.Set;

/**
 * Model class for containing detail of lineage
 * Nodes and target of node in any lineage graph
 *
 * @author Ketan on 5/27/2017.
 */

@Data
public class HiveLineage extends AtlasEntity {

    private JsonNode propJson;
    private JsonNode tagJson;
    private JsonNode auditJson;
    private JsonNode schemaJson;
    List<EdgeData> edges;
    Set<NodeData> nodes;

}

//Redundant..remove once UI works
@Data
class EdgeData {
    AtlasEdge data;
}

@Data
class AtlasEdge {
    String id;
    String source;
    String target;

}

@Data
class NodeData {
    Node data;

    public NodeData(Node node) {
        this.data = node;
    }
}

@Data
class Node {
    private String id;
    private String name;
    private String type;
    private String query;
    private boolean isTarget;


    public Node(String nodeId, String name, String typeName, String query, boolean isTarget) {
        this.id = nodeId;
        this.name = name;
        this.type = typeName;
        this.query = query;
        this.isTarget = isTarget;
    }
}