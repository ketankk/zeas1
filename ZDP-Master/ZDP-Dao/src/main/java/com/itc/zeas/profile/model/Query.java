package com.itc.zeas.profile.model;

import lombok.Data;

/**
 * this Query class is for Hive Query editor
 * Ui is passing Query and EntityId as post parameter
 * EntityId is used to get TableName
 *
 * @author 20597
 *         Jan 17, 2017
 */
@Data
public class Query {
    private String entityId;
    private String query;


}
