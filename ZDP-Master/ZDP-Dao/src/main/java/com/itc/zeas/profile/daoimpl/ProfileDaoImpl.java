package com.itc.zeas.profile.daoimpl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itc.zeas.profile.model.Profile;
import com.itc.zeas.project.extras.ZDPDaoConstant;
import com.itc.zeas.usermanagement.model.UserLevelPermission;
import com.itc.zeas.usermanagement.model.UserManagementConstant;
import com.itc.zeas.usermanagement.model.ZDPUserAccess;
import com.itc.zeas.usermanagement.model.ZDPUserAccessImpl;
import com.itc.zeas.utility.connection.ConnectionUtility;
import com.zdp.dao.ZDPDataAccessObjectImpl;
import com.itc.zeas.profile.model.Entity;
import com.itc.zeas.utility.utility.UserProfileStatusCache;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Ketan on 4/29/2017.
 */
public class ProfileDaoImpl {
    private static final Logger LOGGER = Logger.getLogger(ProfileDaoImpl.class);


    public List<Profile> getProfiles(String userId) throws Exception {
        List<Profile> profileList = new ArrayList<>();

        ZDPUserAccess zdpUserAccess = new ZDPUserAccessImpl();
        Map<String, Integer> userNamePermissionMap = zdpUserAccess.getUserNamePermissionMap(userId);
        UserLevelPermission userLevelPermission = zdpUserAccess.getUserLevelPermission(userId);
        int userLevelDatasetPermission = userLevelPermission.getDatasetPermission();


        String userNameList = "'" + userId + "'";
        for (String username : userNamePermissionMap.keySet()) {
            userNameList = userNameList + ",'" + username + "'";
        }
        Connection connection = ConnectionUtility.getConnection();

        //TODO change this to prepare statement and use fom @sqlEditor file
        String sQuery = "select * from entity where CREATED_BY in(" + userNameList + ") and type in ('DataIngestion','bulk') ";

        PreparedStatement preparedStatement = connection.prepareStatement(sQuery);
        ResultSet rs = preparedStatement.executeQuery();
        while (rs.next()) {
            if (!rs.getString("type").equalsIgnoreCase("bulk")) {

                Profile profile = new Profile();
                int schedulerId = rs.getInt("id");
                profile.setScedulerID(schedulerId);
                String jobStatus = getJobStatus(schedulerId);
                profile.setJobStatus(jobStatus);

                String name = rs.getString("name");
                profile.setName(name);

                String createdby = rs.getString("CREATED_BY");
                profile.setCreatedby(createdby);


                if (createdby.equals(userId)) {
                    // creator is requesting user
                    profile.setPermissionLevel(userLevelDatasetPermission);
                } else {
                    Integer groupLevelpermission = userNamePermissionMap.get(createdby);
                    profile.setPermissionLevel((groupLevelpermission & userLevelDatasetPermission));
                }

                // added by deepak usermanagement 1 end

                String jsonBlob = rs.getString("json_data");
                // converting jsonblob string to json object using jackson
                // libary
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(jsonBlob);
                String dataSourceLoc = (rootNode.get("dataSource")).textValue();
                profile.setSourcePath(dataSourceLoc);

                String destinationDatasetName = (rootNode.get("destinationDataset")).textValue();

                profile.setSchedulerFrequency(rootNode.get("frequency").textValue());
                LOGGER.debug("dataSourceLoc: " + dataSourceLoc);

                Entity entityFromDataSource = getEntityByName(dataSourceLoc);
                String enityJsonBlob = entityFromDataSource.getJsonblob();
                LOGGER.debug("entityFromDataSource json blob: " + entityFromDataSource.getJsonblob());
                if (enityJsonBlob != null) {
                    rootNode = mapper.readTree(enityJsonBlob);
                    if (!rootNode.get("sourcerType").textValue().equalsIgnoreCase("RDBMS")) {
                        JsonNode locationNode = rootNode.get("location");
                        if (locationNode != null) {
                            String sourceLocation = locationNode.textValue();
                            profile.setSourcePath(sourceLocation);
                        }
                    }
                    profile.setType(entityFromDataSource.getFormat());
                    profile.setDatasourceid((int) entityFromDataSource.getId());
                    String schemaName = null;
                    try {
                        schemaName = (rootNode.get("schema")).textValue();
                        String sourceFormat = (rootNode.get("format")).textValue();
                        profile.setSourceFormat(sourceFormat);
                    } catch (Exception e) {
                        LOGGER.debug("exception while retrieving schema: " + enityJsonBlob);
                    }
                    //
                    if (schemaName != null) {

                        Entity entityForGivenSchemaName = getEntityByName(schemaName);
                        profile.setUser(entityForGivenSchemaName.getCreatedBy());
                        profile.setSchemaId((int) entityForGivenSchemaName.getId());
                        profile.setSchemaName(entityForGivenSchemaName.getName());
                        String schemaEntityJsonBlob = entityForGivenSchemaName.getJsonblob();
                        profile.setSchemaJsonBlob(schemaEntityJsonBlob);
                        Date modificationDate = entityForGivenSchemaName.getUpdatedDate();
                        profile.setSchemaModificationDate(modificationDate);
                    }
                }

                LOGGER.debug("datasetDestinationLoc: " + destinationDatasetName);
                Entity entityFromDataDestination = getEntityByName(destinationDatasetName);
                if (entityFromDataDestination.getJsonblob() != null) {
                    rootNode = mapper.readTree(entityFromDataDestination.getJsonblob());
                    String destinationLocation = (rootNode.get("location")).textValue();
                    profile.setDatasetID((int) entityFromDataDestination.getId());
                    profile.setDataSetTargetPath(destinationLocation);
                }
                profile.setDatasetName(destinationDatasetName);
                profileList.add(profile);
            } else {

                Profile profile = new Profile();
                String name = rs.getString("name");
                profile.setName(name);
                profile.setSchemaName(name);
                JSONObject jsonobj = new JSONObject();
                jsonobj.put("schemaJsonBlob", name);
                profile.setSchemaJsonBlob(jsonobj.toString());
                String type = rs.getString("type");
                profile.setType(type);
                profile.setSourceFormat(type);
                String createdby = rs.getString("CREATED_BY");
                profile.setCreatedby(createdby);
                Entity entity = getEntityByName(name);
                Date modificationDate = entity.getUpdatedDate();
                profile.setSchemaModificationDate(modificationDate);
                Entity entityAdmin = getEntityByName(name);
                Date lastModified = entityAdmin.getUpdatedDate();
                profile.setSchemaModificationDate(lastModified);
                profileList.add(profile);


            }
        }
        return profileList;

    }
public Map<Long, String> getRunStatus(String userId) throws Exception {
    LOGGER.debug("inside function getProfileRunStatus");
    Map<Long, String> runStatus = new HashMap<>();
    ZDPUserAccess zdpUserAccess = new ZDPUserAccessImpl();
    LOGGER.info("Getting user details for listing datasets");

    String sQuery;
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    ResultSet rs = null;

    Map<String, Integer> userNamePermissionMap = zdpUserAccess.getUserNamePermissionMap(userId);
    String userNameList = "";
    for (String username : userNamePermissionMap.keySet()) {
        userNameList = userNameList + ",'" + username + "'";
    }
    userNameList = userNameList.substring(1);
    System.out.println("userNameList: " + userNameList);
    connection = ConnectionUtility.getConnection();
    sQuery = "select id,type from entity where type='DataIngestion' and CREATED_BY in(" + userNameList + ")";
    try {
        preparedStatement = connection.prepareStatement(sQuery);
        rs = preparedStatement.executeQuery();
        while (rs.next()) {
            Long entityId = rs.getLong("id");
            String entityType = rs.getString("type");
            if (entityType.equalsIgnoreCase("DataIngestion")) {
                String jobStatus = getJobStatus(entityId);
                runStatus.put(entityId, jobStatus);
            }
        }
    } catch (SQLException | IOException e) {
        e.printStackTrace();
    } catch (Exception e) {


    } finally {
        ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
    }
return runStatus;
}
    public Entity getEntityByName(String name) throws Exception {

        Entity entity = new Entity();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            String sQuery = ConnectionUtility.getSQlProperty("GET_ENTITY_BY_NAME");
            connection = ConnectionUtility.getConnection();
            preparedStatement = connection.prepareStatement(sQuery);
            preparedStatement.setString(1, name);
            rs = preparedStatement.executeQuery();
            while (rs.next()) {
                entity.setId(rs.getInt("id"));
                entity.setName(rs.getString("name"));
                entity.setType(rs.getString("type"));
                entity.setJsonblob(rs.getString("json_data"));
                entity.setActive(rs.getBoolean("is_active"));
                entity.setCreatedBy(rs.getString("createdBy"));
                entity.setCreatedDate(rs.getTimestamp("created"));
                entity.setUpdatedBy(rs.getString("updatedBy"));
                try {
                    entity.setUpdatedDate(rs.getTimestamp("last_modified"));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                LOGGER.debug("name:" + name);
                // logger.debug("entity date:" +
                // entity.getUpdatedTimestamp().getTime());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
        }
        // finally {
        // closeConnection(connection);
        // }

        return entity;
    }

    public static String getJobStatus(long ingestionJobId) throws Exception {
        String jobStatus = "New";
        Connection connection = null;
        String sQuery = ConnectionUtility.getSQlProperty("GET_INGESTION_JOB_STATUS");
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            connection = ConnectionUtility.getConnection();
            preparedStatement = connection.prepareStatement(sQuery);
            preparedStatement.setLong(1, ingestionJobId);
            rs = preparedStatement.executeQuery();
            if (rs.next()) {
                jobStatus = rs.getObject(1).toString();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
        }
        return jobStatus;
    }

    //TODO check for bug or improvement
    public List<Profile> getProfilesForAdminUser() throws Exception {
        LOGGER.debug("inside function getProfilesForAdminUser");
        List<Profile> profileList = new ArrayList<Profile>();
        String sQuery;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            connection = ConnectionUtility.getConnection();
            sQuery = ConnectionUtility.getSQlProperty("LIST_ENTITY_ADMIN");
            preparedStatement = connection.prepareStatement(sQuery);
            rs = preparedStatement.executeQuery();
            while (rs.next()) {

                if (!rs.getString("type").equalsIgnoreCase("bulk")) {
                    Profile profile = new Profile();
                    int schedulerId = rs.getInt("id");
                    profile.setScedulerID(schedulerId);
                    String jobStatus = getJobStatus(schedulerId);
                    profile.setJobStatus(jobStatus);
                    String name = rs.getString("name");
                    profile.setName(name);
                    String createdby = rs.getString("CREATED_BY");
                    profile.setCreatedby(createdby);
                    profile.setPermissionLevel(UserManagementConstant.READ_WRITE_EXECUTE);
                    String jsonBlob = rs.getString("json_data");
                    // converting jsonblob string to json object using jackson
                    // libary
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode rootNode = mapper.readTree(jsonBlob);
                    String dataSourceLoc = (rootNode.get("dataSource")).textValue();
                    profile.setSourcePath(dataSourceLoc);

                    String destinationDatasetName = (rootNode.get("destinationDataset")).textValue();

                    profile.setSchedulerFrequency(rootNode.get("frequency").textValue());
                    LOGGER.debug("dataSourceLoc: " + dataSourceLoc);
                    Entity entityFromDataSource = getEntityByName(dataSourceLoc);
                    String enityJsonBlob = entityFromDataSource.getJsonblob();
                    LOGGER.debug("entityFromDataSource json blob: " + entityFromDataSource.getJsonblob());
                    if (enityJsonBlob != null) {
                        rootNode = mapper.readTree(enityJsonBlob);
                        if (!rootNode.get("sourcerType").textValue().equalsIgnoreCase("RDBMS")) {
                            JsonNode locationNode = rootNode.get("location");
                            if (locationNode != null) {
                                String sourceLocation = locationNode.textValue();
                                profile.setSourcePath(sourceLocation);
                            }
                        }
                        profile.setType(entityFromDataSource.getFormat());
                        profile.setDatasourceid((int) entityFromDataSource.getId());
                        String schemaName = null;
                        try {
                            schemaName = (rootNode.get("schema")).textValue();
                            String sourceFormat = (rootNode.get("format")).textValue();
                            profile.setSourceFormat(sourceFormat);
                        } catch (Exception e) {
                            LOGGER.debug("exception while retrieving schema: " + enityJsonBlob);
                        }
                        //
                        if (schemaName != null) {

                            Entity entityForGivenSchemaName = getEntityByName(schemaName);
                            profile.setUser(entityForGivenSchemaName.getCreatedBy());
                            profile.setSchemaId((int) entityForGivenSchemaName.getId());
                            profile.setSchemaName(entityForGivenSchemaName.getName());
                            String schemaEntityJsonBlob = entityForGivenSchemaName.getJsonblob();
                            profile.setSchemaJsonBlob(schemaEntityJsonBlob);
                            Date modificationDate = entityForGivenSchemaName.getUpdatedDate();
                            profile.setSchemaModificationDate(modificationDate);

                        }
                    }

                    LOGGER.debug("datasetDestinationLoc: " + destinationDatasetName);
                    Entity entityFromDataDestination = getEntityByName(destinationDatasetName);
                    if (entityFromDataDestination.getJsonblob() != null) {
                        rootNode = mapper.readTree(entityFromDataDestination.getJsonblob());
                        String destinationLocation = (rootNode.get("location")).textValue();
                        profile.setDatasetID((int) entityFromDataDestination.getId());
                        profile.setDataSetTargetPath(destinationLocation);
                    }
                    profile.setDatasetName(destinationDatasetName);
                    profileList.add(profile);
                } else {
                    Profile profile = new Profile();
                    String name = rs.getString("name");
                    profile.setName(name);
                    profile.setSchemaName(name);
                    JSONObject jsonobj = new JSONObject();
                    jsonobj.put("schemaJsonBlob", name);
                    profile.setSchemaJsonBlob(jsonobj.toString());
                    String type = rs.getString("type");
                    profile.setType(type);
                    profile.setSourceFormat(type);
                    String createdby = rs.getString("CREATED_BY");
                    profile.setCreatedby(createdby);
                    profileList.add(profile);
                    Entity entity = getEntityByName(name);
                    Date modificationDate = entity.getUpdatedDate();
                    profile.setSchemaModificationDate(modificationDate);
                    Entity entityAdmin = getEntityByName(name);
                    Date lastModified = entityAdmin.getUpdatedDate();
                    profile.setSchemaModificationDate(lastModified);

                }

            }
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
        }

        List<Profile> profileListToBeSorted = new ArrayList<Profile>();
        List<Profile> profileListToBeAppended = new ArrayList<Profile>();
        for (Profile profile : profileList) {
            if (profile.getSchemaModificationDate() != null) {
                profileListToBeSorted.add(profile);
            } else {
                profileListToBeAppended.add(profile);
            }
        }
        Collections.sort(profileListToBeSorted);
        for (Profile profile : profileListToBeAppended) {
            profileListToBeSorted.add(profile);
        }
        return profileListToBeSorted;
    }
    /**
     * this method is to add entity to database
     *
     * @param entity
     * @throws Exception
     * @throws SQLException
     */
    public void addEntity(Entity entity, String userName)
            throws Exception {


        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            String sQuery = ConnectionUtility.getSQlProperty("INSERT_ENTITY");
            connection = ConnectionUtility.getConnection();
            preparedStatement = connection.prepareStatement(sQuery);
            preparedStatement.setString(1, entity.getName());
            preparedStatement.setString(2, entity.getType());
            preparedStatement.setString(3, entity.getJsonblob());

            // Currently sending '1' as isActive value
            preparedStatement.setBoolean(4, true);
            preparedStatement.setString(5, entity.getCreatedBy());
            preparedStatement.setString(6, entity.getUpdatedBy());
            preparedStatement.executeUpdate();

            if (entity.getType().equalsIgnoreCase("dataschema")) {

                // Update cached map to notify that ingestion profile is
                // successfully created
                UserProfileStatusCache.updateMap(userName + "-" + entity.getName(), true);

                ZDPDataAccessObjectImpl accessImpl = new ZDPDataAccessObjectImpl();
                List<String> userList = new ArrayList<>();
                userList.add(userName);
                accessImpl.addActivitiesBatchForNewAPI(entity.getName(),
                        "New ingestion profile '" + entity.getName() + "' created by " + userName,
                        ZDPDaoConstant.INGESTION_ACTIVITY, ZDPDaoConstant.CREATE_ACTIVITY, userList, userName);
                LOGGER.info("User " + userName + ": New ingestion profile '" + entity.getName()
                        + "' created successfully.");

            }
			/*
			 * we will get entity id by querying with entity name
			 */


        } catch (SQLException e) {
            LOGGER.info("EntityManager.addEntity(): SQLException: " + e.getMessage());
            LOGGER.info("User " + userName + ": New ingestion profile '" + entity.getName() + "' failed.");
        } finally {
            ConnectionUtility.releaseConnectionResources(preparedStatement, connection);
        }

    }
}
