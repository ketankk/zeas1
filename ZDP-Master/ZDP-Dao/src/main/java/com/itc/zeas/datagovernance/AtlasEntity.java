package com.itc.zeas.datagovernance;

import lombok.Data;

import java.util.List;
import java.util.Set;

/**
 * @author Ketan on 5/30/2017.
 */
@Data
public class AtlasEntity {

	private String entityName;
	private String entityOwner;
	private String entityDesc;
	private String entityGuid;
	private String entityType;
	private Set<String> targetNodesid;
	private List<String> tagList;

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		AtlasEntity that = (AtlasEntity) o;
		return entityGuid.equals(that.entityGuid);
	}

	@Override
	public int hashCode() {
		return entityGuid.hashCode();
	}

	public AtlasEntity(String entityGuid) {
		super();
		this.entityGuid = entityGuid;
	}

	public AtlasEntity() {
		super();
		// TODO Auto-generated constructor stub
	}
}
