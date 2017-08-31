package com.itc.zeas.profile;

import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.exceptions.ZeasSQLException;
import com.itc.zeas.ingestion.automatic.bulk.EntityManagerInterface;
import com.itc.zeas.ingestion.model.ArchivedFileInfo;
import com.itc.zeas.machineLearning.model.MLAnalysis;
import com.itc.zeas.profile.model.BulkEntity;
import com.itc.zeas.profile.model.Entity;
import com.itc.zeas.profile.model.Profile;
import com.itc.zeas.project.extras.ZDPDaoConstant;
import com.itc.zeas.usermanagement.model.UserLevelPermission;
import com.itc.zeas.usermanagement.model.UserManagementConstant;
import com.itc.zeas.usermanagement.model.ZDPUserAccess;
import com.itc.zeas.usermanagement.model.ZDPUserAccessImpl;
import com.itc.zeas.utility.connection.ConnectionUtility;
import com.itc.zeas.utility.utils.CommonUtils;
import com.taphius.dataloader.DataLoader;
import com.zdp.dao.ZDPDataAccessObject;
import com.zdp.dao.ZDPDataAccessObjectImpl;
import com.itc.zeas.model.DataIngestionLog;
import com.itc.zeas.model.DatasetPathDetails;
import com.itc.zeas.model.PipelineStageLog;
import com.itc.zeas.model.ProcessedPipeline;
import com.itc.zeas.utility.utility.ConfigurationReader;
import com.itc.zeas.utility.utility.UserProfileStatusCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map.Entry;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Ketan on 5/1/2017.
 */
@Service(value="entityManager")

public class EntityManager implements EntityManagerInterface{
    private static String delete;
    // private Connection connection;
    private static final Logger LOGGER = Logger.getLogger(EntityManager.class);
    private String hdfsPath;

    public EntityManager() {
    }

    /**
     * this method is used to list all entities
     *
     * @param type
     * @return List of Entity
     * @throws Exception
     * @throws SQLException
     */
    public List<Entity> getEntity(String type, HttpServletRequest httpRequest) throws Exception {
        List<Entity> entities = new ArrayList<Entity>();

        // modification from Deepak starts
        CommonUtils commonUtils = new CommonUtils();
        String userId = commonUtils.extractUserNameFromRequest(httpRequest);
        ZDPUserAccess zdpUserAccess = new ZDPUserAccessImpl();
        Boolean isSuperUser = zdpUserAccess.isSuperUser(userId);
        String sQuery = null;

        if (!isSuperUser) {
            Map<String, Integer> userNamePermissionMap = zdpUserAccess.getUserNamePermissionMap(userId);
            // filter map for execute permission
            Iterator<Entry<String, Integer>> iterator = userNamePermissionMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, Integer> entry = iterator.next();
                Integer permission = entry.getValue();
                if (!(permission == UserManagementConstant.READ_EXECUTE
                        || permission == UserManagementConstant.READ_WRITE_EXECUTE)) {
                    iterator.remove();
                }
            }
            // String userNameList = "";
            String userNameList = "'" + userId + "'";
            for (String username : userNamePermissionMap.keySet()) {
                userNameList = userNameList + ",'" + username + "'";
            }
            // userNameList = userNameList.substring(1);
            System.out.println("userNameList: " + userNameList);
            // added by deepak usermanager1 end

            // sQuery = DBUtility.getSQlProperty("LIST_ENTITY_NEW_1");//
            // LIST_ENTITY
            sQuery = "select * from entity where CREATED_BY in(" + userNameList + ") and type='" + type + "'";
        }


        // modification from Deepak Ends
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            connection = ConnectionUtility.getConnection();
            if (isSuperUser) {
                LOGGER.debug("user is super user");
                sQuery = ConnectionUtility.getSQlProperty("LIST_ENTITY");
                preparedStatement = connection.prepareStatement(sQuery);
                preparedStatement.setString(1, type);
            } else {
                preparedStatement = connection.prepareStatement(sQuery);
            }

            rs = preparedStatement.executeQuery();
            while (rs.next()) {
                Entity entity = new Entity();
                entity.setId(rs.getInt("id"));
                entity.setName(rs.getString("name"));
                entity.setType(rs.getString("type"));
                entity.setJsonblob(rs.getString("json_data"));//
                System.out.println("type: " + type);
                System.out.println("json_data: " + rs.getString("json_data"));

                //
                entity.setActive(rs.getBoolean("is_active"));
                entity.setCreatedBy(rs.getString("created_by"));
                entity.setUpdatedBy(rs.getString("updated_by"));
                entity.setCreatedDate(rs.getTimestamp("created"));
				/*
				 * try {
				 * entity.setCreatedDate(rs.getTimestamp("last_modified")); }
				 * catch (SQLException e) { //e.printStackTrace(); }
				 */
                entities.add(entity);
            }
        } catch (SQLException e) {
            LOGGER.error("SQLException while executing sql query string " + sQuery);
            e.printStackTrace();
        } finally {
            ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
        }
        // finally {
        // closeConnection(connection);
        // }

        return entities;
    }

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

                if(!rs.getString("type").equalsIgnoreCase("bulk")){
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
                    String dataSourceLoc = (rootNode.get("dataSource")).getTextValue();
                    profile.setSourcePath(dataSourceLoc);

                    String destinationDatasetName = (rootNode.get("destinationDataset")).getTextValue();

                    profile.setSchedulerFrequency(rootNode.get("frequency").getTextValue());
                    LOGGER.debug("dataSourceLoc: " + dataSourceLoc);
                    Entity entityFromDataSource = getEntityByName(dataSourceLoc);
                    String enityJsonBlob = entityFromDataSource.getJsonblob();
                    LOGGER.debug("entityFromDataSource json blob: " + entityFromDataSource.getJsonblob());
                    if (enityJsonBlob != null) {
                        rootNode = mapper.readTree(enityJsonBlob);
                        if (!rootNode.get("sourcerType").getTextValue().equalsIgnoreCase("RDBMS")) {
                            JsonNode locationNode = rootNode.get("location");
                            if (locationNode != null) {
                                String sourceLocation = locationNode.getTextValue();
                                profile.setSourcePath(sourceLocation);
                            }
                        }
                        profile.setType(entityFromDataSource.getFormat());
                        profile.setDatasourceid((int) entityFromDataSource.getId());
                        String schemaName = null;
                        try {
                            schemaName = (rootNode.get("schema")).getTextValue();
                            String sourceFormat = (rootNode.get("format")).getTextValue();
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
                        String destinationLocation = (rootNode.get("location")).getTextValue();
                        profile.setDatasetID((int) entityFromDataDestination.getId());
                        profile.setDataSetTargetPath(destinationLocation);
                    }
                    profile.setDatasetName(destinationDatasetName);
                    profileList.add(profile);
                }
                else{
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
        // finally {
        // closeConnection(connection);
        // }
        // Collections.sort(profileList);
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
     * This method is used to list all Profiles, every Profile is representation
     * of consolidated information about data source, data set,schema and
     * scheduler
     *
     * @return List of Profile
     * @throws Exception
     * @throws SQLException
     */
    public List<Profile> getProfiles(HttpServletRequest httpServletRequest) throws Exception {
        LOGGER.debug("inside function getProfile");

        CommonUtils commonUtils = new CommonUtils();
        String accessToken = commonUtils.extractAuthTokenFromRequest(httpServletRequest);
        String userId = commonUtils.getUserNameFromToken(accessToken);
        ZDPUserAccess zdpUserAccess = new ZDPUserAccessImpl();
        Boolean isSuperUser = zdpUserAccess.isSuperUser(userId);
        if (isSuperUser) {
            LOGGER.debug("user is super user");
            return getProfilesForAdminUser();
        }
        LOGGER.info("Getting user details for listing datasets");
        List<Profile> profileList = new ArrayList<Profile>();

        String sQuery;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {


            // added by deepak usermanager1 start

            Map<String, Integer> userNamePermissionMap = zdpUserAccess.getUserNamePermissionMap(userId);
            // String userNameList = "";
            /**
             * To handle scenario where user is not part of any group Fix for
             * bug Id-94
             */
            String userNameList = "'" + userId + "'";
            for (String username : userNamePermissionMap.keySet()) {
                userNameList = userNameList + ",'" + username + "'";
            }
            // userNameList = userNameList.substring(1);
            System.out.println("userNameList: " + userNameList);
            // added by deepak usermanager1 end

            // fetch logged in User dataset permission mapping
            UserLevelPermission userLevelPermission = zdpUserAccess.getUserLevelPermission(userId);
            int userLevelDatasetPermission = userLevelPermission.getDatasetPermission();

            connection = ConnectionUtility.getConnection();
            // sQuery = DBUtility.getSQlProperty("LIST_ENTITY_NEW_1");//
            // LIST_ENTITY
            sQuery = "select * from entity where CREATED_BY in(" + userNameList + ") and type in ('DataIngestion','bulk') ";


            // updated
            // to
            // LIST_ENTITY_NEW
            preparedStatement = connection.prepareStatement(sQuery);

            // added by deepak usermanager1 start
            // preparedStatement.setString(1, userNameList);
            // System.out.println("######################################################################"+preparedStatement.toString());
            // added by deepak usermanager1 end

            // preparedStatement.setString(1, "dataIngestion");
            // preparedStatement.setString(1, userId);
            rs = preparedStatement.executeQuery();
            while (rs.next()) {
                if(!rs.getString("type").equalsIgnoreCase("bulk"))
                {

                    Profile profile = new Profile();
                    int schedulerId = rs.getInt("id");
                    profile.setScedulerID(schedulerId);

                    // added by deepak starts to add permission level in profiles
                    // Integer permissionLevel = entityIdPermissionMap
                    // .get(schedulerId);
                    // profile.setPermissionLevel(permissionLevel);
                    // added by deepak ends

                    String jobStatus = getJobStatus(schedulerId);
                    profile.setJobStatus(jobStatus);

                    String name = rs.getString("name");
                    profile.setName(name);

                    String createdby = rs.getString("CREATED_BY");
                    profile.setCreatedby(createdby);

                    // added by deepak usermanagement 1 start
                    // Integer groupLevelpermission = userNamePermissionMap
                    // .get(createdby);
                    // profile.setPermissionLevel(groupLevelpermission);

                    if (createdby.equals(userId)) {
                        // creator is requesting user
                        profile.setPermissionLevel(userLevelDatasetPermission);
                    } else {
                        Integer groupLevelpermission = userNamePermissionMap.get(createdby);
                        // if (groupLevelpermission > userLevelDatasetPermission) {
                        // profile.setPermissionLevel(userLevelDatasetPermission);
                        // } else {
                        // profile.setPermissionLevel(groupLevelpermission);
                        // }
                        profile.setPermissionLevel((groupLevelpermission & userLevelDatasetPermission));
                    }

                    // added by deepak usermanagement 1 end

                    String jsonBlob = rs.getString("json_data");
                    // converting jsonblob string to json object using jackson
                    // libary
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode rootNode = mapper.readTree(jsonBlob);
                    String dataSourceLoc = (rootNode.get("dataSource")).getTextValue();
                    profile.setSourcePath(dataSourceLoc);

                    String destinationDatasetName = (rootNode.get("destinationDataset")).getTextValue();

                    profile.setSchedulerFrequency(rootNode.get("frequency").getTextValue());
                    LOGGER.debug("dataSourceLoc: " + dataSourceLoc);

                    Entity entityFromDataSource = getEntityByName(dataSourceLoc);
                    String enityJsonBlob = entityFromDataSource.getJsonblob();
                    LOGGER.debug("entityFromDataSource json blob: " + entityFromDataSource.getJsonblob());
                    if (enityJsonBlob != null) {
                        rootNode = mapper.readTree(enityJsonBlob);
                        if (!rootNode.get("sourcerType").getTextValue().equalsIgnoreCase("RDBMS")) {
                            JsonNode locationNode = rootNode.get("location");
                            if (locationNode != null) {
                                String sourceLocation = locationNode.getTextValue();
                                profile.setSourcePath(sourceLocation);
                            }
                        }
                        profile.setType(entityFromDataSource.getFormat());
                        profile.setDatasourceid((int) entityFromDataSource.getId());
                        String schemaName = null;
                        try {
                            schemaName = (rootNode.get("schema")).getTextValue();
                            String sourceFormat = (rootNode.get("format")).getTextValue();
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
                        String destinationLocation = (rootNode.get("location")).getTextValue();
                        profile.setDatasetID((int) entityFromDataDestination.getId());
                        profile.setDataSetTargetPath(destinationLocation);
                    }
                    profile.setDatasetName(destinationDatasetName);
                    profileList.add(profile);
                }
                else{

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
        // finally {
        // closeConnection(connection);
        // }
        // Collections.sort(profileList);
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
    public void addEntity(Entity entity, HttpServletRequest request)
            throws Exception {

        CommonUtils commonUtils = new CommonUtils();
        String accessToken = commonUtils.extractAuthTokenFromRequest(request);
        String userName = commonUtils.getUserNameFromToken(accessToken);
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            String sQuery = ConnectionUtility.getSQlProperty("INSERT_ENTITY");
            // String entityIdQuery = DBUtility.getSQlProperty("GET_ENTITY_ID");
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
                // ZDPDaoUtility.addActivities(entity.getName(), connection,
                // userName, );
            }
			/*
			 * we will get entity id by querying with entity name
			 */

			/*
			 * if (entity.getType().equalsIgnoreCase("dataingestion")) {
			 * ZDPUserAccess userAccess = new ZDPUserAccessImpl(); // int
			 * permission = UserManagementConstant.READ_WRITE_EXECUTE; // //
			 * userName and groupName is Same and Permission set to 7 // which
			 * // // is Max
			 *
			 * // userAccess.addEntryInDatasetPermission(entityIdQuery, //
			 * entity.getName(), userName, permission); int entityId = 0; try {
			 * PreparedStatement entityIdStatement = connection
			 * .prepareStatement(entityIdQuery); entityIdStatement.setString(1,
			 * entity.getName()); ResultSet rs =
			 * entityIdStatement.executeQuery(); while (rs.next()) { entityId =
			 * rs.getInt(1); } } catch (SQLException e) { e.printStackTrace(); }
			 * userAccess.addEntryInResPermissionForDefaultUGroup(
			 * UserManagementConstant.ResourceType.DATASET, new Long( entityId),
			 * userName); }
			 */
        } catch (SQLException e) {
            LOGGER.info("EntityManager.addEntity(): SQLException: " + e.getMessage());
            LOGGER.info("User " + userName + ": New ingestion profile '" + entity.getName() + "' failed.");
        } finally {
            ConnectionUtility.releaseConnectionResources(preparedStatement, connection);
        }
        // finally {
        // closeConnection(connection);
        // }
    }

    /**
     * this method is to add entity to database
     *
     * @throws Exception
     * @throws SQLException
     */
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
                entity.setCreatedBy(rs.getString("created_by"));
                entity.setCreatedDate(rs.getTimestamp("created"));
                entity.setUpdatedBy(rs.getString("updated_by"));
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

    public BulkEntity getBulkEntityByName(String name) throws Exception {

		BulkEntity bulkEntity = new BulkEntity();
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			String sQuery = ConnectionUtility.getSQlProperty("GET_BULK_ENTITY_BY_NAME");
			connection = ConnectionUtility.getConnection();
			preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, name);
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				bulkEntity.setId(rs.getInt("id"));
				bulkEntity.setType("Bulk");
				bulkEntity.setName(rs.getString("jobname"));
				bulkEntity.setJsonblob(rs.getString("json_data_schema"));
				bulkEntity.setJsonblobSchema(rs.getString("json_data_schema"));
				bulkEntity.setJsonblobDataset(rs.getString("json_data_dataset"));
				bulkEntity.setJsonblobSource(rs.getString("json_data_source"));
				bulkEntity.setActive(rs.getBoolean("is_active"));
				bulkEntity.setCreatedBy(rs.getString("created_by"));
				bulkEntity.setCreatedDate(rs.getTimestamp("created"));
				bulkEntity.setUpdatedBy(rs.getString("updated_by"));
				try {
					bulkEntity.setUpdatedDate(rs.getTimestamp("last_modified"));
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

		return bulkEntity;
	}

    /**
     * this method is to update entity details
     *
     *            ds
     * @param type
     * @param entityId
     * @throws Exception
     * @throws SQLException
     */
    public void updateEntity(Entity ds, String type, Long entityId,
                             HttpServletRequest httpRequest,
                             HttpServletResponse httpServletResponse) throws Exception {

        // UPDATE ENTITY SET NAME = ? , JSON_DATA = ?,IS_ACTIVE =?,
        // UPDATED_BY = ?,LAST_MODIFIED = NOW() WHERE ID = ? AND TYPE = ?

        // added by deepak starts checking user authenticity to update entity
        CommonUtils commonUtils = new CommonUtils();
        String userName = commonUtils.extractUserNameFromRequest(httpRequest);
        ZDPUserAccess zdpUserAccess = new ZDPUserAccessImpl();
        Boolean haveValidPermission = zdpUserAccess.validateUserPermissionForResource(
                UserManagementConstant.ResourceType.DATASET, userName, entityId, UserManagementConstant.READ_WRITE);
        if (!haveValidPermission) {
            LOGGER.info("User " + userName + ": doesn't have enough permission to update the entity with entity id "
                    + entityId);
            httpServletResponse.setStatus(403);
            try {
                httpServletResponse.getWriter().print("Failed ! Not have enough permission to update ");
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            // added by deepak ends
            Connection connection = null;
            PreparedStatement preparedStatement = null;
            try {
                String sQuery = ConnectionUtility.getSQlProperty("UPDATE_ENTITY");
                connection = ConnectionUtility.getConnection();
                preparedStatement = connection.prepareStatement(sQuery);
                preparedStatement.setString(1, ds.getName());
                preparedStatement.setString(2, ds.getJsonblob());
                // preparedStatement.setBoolean(3,ds.isActive());
                preparedStatement.setString(3, ds.getUpdatedBy());
                preparedStatement.setLong(4, entityId);
                preparedStatement.setString(5, type);
                preparedStatement.executeUpdate();
                if (ds.getType().equalsIgnoreCase("dataschema")) {
                    ZDPDataAccessObjectImpl accessImpl = new ZDPDataAccessObjectImpl();
                    List<String> userList = new ArrayList<>();
                    userList.add(userName);
                    accessImpl.addActivitiesBatchForNewAPI(ds.getName(),
                            "Ingestion profile '" + ds.getName() + "' updated by " + userName,
                            ZDPDaoConstant.INGESTION_ACTIVITY, ZDPDaoConstant.UPDATE_ACTIVITY, userList, userName);
                    LOGGER.info(
                            "User " + userName + ": Successful updation of ingestion profile '" + ds.getName() + "'.");
                }
            } catch (SQLException e) {
                e.printStackTrace();
                LOGGER.info(
                        "User " + userName + ": Unsuccessful updation of ingestion profile '" + ds.getName() + "'.");
            } finally {
                ConnectionUtility.releaseConnectionResources(preparedStatement, connection);
            }
        }
        // finally {
        // closeConnection(connection);
        // }

    }

    /**
     * This method is to fetch entity details for a given entityId
     *
     * @param type
     * @param id
     * @return Entity
     * @throws Exception
     * @throws SQLException
     */
    public Entity getEntityById(String type, Long id,
                                HttpServletRequest httpRequest) throws Exception {

        Entity entity = new Entity();
        // added by deepak for authorization check starts
        Boolean haveValidPermission = false;

        CommonUtils commonUtils = new CommonUtils();
        String userName = commonUtils.extractUserNameFromRequest(httpRequest);
        ZDPUserAccess zdpUserAccess = new ZDPUserAccessImpl();
        haveValidPermission = zdpUserAccess.validateUserPermissionForResource(
                UserManagementConstant.ResourceType.DATASET, userName, id, UserManagementConstant.READ);
        if (haveValidPermission) {
            // added by deepak for authorization check ends
            Connection connection = null;
            PreparedStatement preparedStatement = null;
            ResultSet rs = null;
            try {
                String sQuery = ConnectionUtility.getSQlProperty("SELECT_ENTITY_BY_ID");
                connection = ConnectionUtility.getConnection();
                preparedStatement = connection.prepareStatement(sQuery);
                preparedStatement.setLong(1, id);
                rs = preparedStatement.executeQuery();
                while (rs.next()) {
                    entity.setId(rs.getInt("id"));
                    entity.setName(rs.getString("name"));
                    entity.setType(rs.getString("type"));
                    entity.setJsonblob(rs.getString("json_data"));
                    entity.setActive(rs.getBoolean("is_active"));
                    entity.setCreatedBy(rs.getString("created_by"));
                    entity.setCreatedDate(rs.getTimestamp("created"));
                    entity.setUpdatedBy(rs.getString("updated_by"));
                    entity.setUpdatedDate(rs.getTimestamp("last_modified"));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
            }
        }
        // finally {
        // closeConnection(connection);
        // }

        return entity;
    }

    /**
     * this method is to delete Entity details
     *
     * @param id
     * @throws Exception
     * @throws SQLException
     */
    public void deleteEntity(Integer id) throws Exception {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            String sQuery = ConnectionUtility.getSQlProperty("DELETE_ENTITY");
            connection = ConnectionUtility.getConnection();
            preparedStatement = connection.prepareStatement(sQuery);
            preparedStatement.setInt(1, id);
            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtility.releaseConnectionResources(preparedStatement, connection);
        }
        // finally {
        // closeConnection(connection);
        // }

    }

    /**
     * DB Call made to fetch the list of particular entity types from DB. Its
     * mostly used to populate UI drop down components.
     *
     * @param entityType
     *            {@link String} Type of Entity.
     * @return {@link List} of Entities of a type.
     * @throws Exception
     * @throws SQLException
     */
    public List<String> listEntity(String entityType) throws Exception {
        List<String> entities = new ArrayList<String>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            String sQuery = ConnectionUtility.getSQlProperty("GET_ENTITY_NAMES");
            connection = ConnectionUtility.getConnection();
            preparedStatement = connection.prepareStatement(sQuery);
            preparedStatement.setString(1, entityType);
            rs = preparedStatement.executeQuery();

            while (rs.next()) {
                entities.add(rs.getString("name"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
        }
        // finally {
        // closeConnection(connection);
        // }
        return entities;
    }

    /**
     * DB Accessor method queries WHITELIST_CONFIG table to fetch the list of
     * items that needs to be populated for particular UI component ex-
     * DataSource Format or DataSource Type etc.
     *
     * @param container
     *            {@link String} name of the Container
     * @param name
     *            {@link String} particular type for given Container.
     * @return {@link List} of String values
     * @throws Exception
     * @throws SQLException
     */
    public List<String> listConfigurations(String container, String name)
            throws Exception {

        List<String> entries = new ArrayList<String>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            String sQuery = ConnectionUtility.getSQlProperty("GET_ATTRIBUTE_NAMES");
            connection = ConnectionUtility.getConnection();
            preparedStatement = connection.prepareStatement(sQuery);
            preparedStatement.setString(1, container);
            preparedStatement.setString(2, name);
            rs = preparedStatement.executeQuery();

            while (rs.next()) {
                entries.add(rs.getString("ENTRY"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
        }
        // finally {
        // closeConnection(connection);
        // }
        return entries;
    }

    /**
     * This method is to fetch ingestion log details
     *
     * @param type
     * @param id
     * @return Entity
     * @throws Exception
     */
    public List<DataIngestionLog> getIngestionDetailsById(Integer id)
            throws Exception {

        List<DataIngestionLog> ingestionLogDtls = new ArrayList<DataIngestionLog>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            String sQuery = ConnectionUtility.getSQlProperty("SELECT_INGESTION_LOG");
            connection = ConnectionUtility.getConnection();
            preparedStatement = connection.prepareStatement(sQuery);
            preparedStatement.setInt(1, id);
            rs = preparedStatement.executeQuery();
            while (rs.next()) {
                DataIngestionLog ingestionLogDtl = new DataIngestionLog();
                ingestionLogDtl.setLogId(rs.getInt("data_ingestion_log_id"));
                ingestionLogDtl.setDataIngestionId(rs.getInt("data_ingestion_id"));
                ingestionLogDtl.setBatch(rs.getString("batch"));
                ingestionLogDtl.setStartTime(rs.getTimestamp("job_start_time"));
                // ingestionLogDtl.setEndTime(rs.getDate("job_end_time"));
                ingestionLogDtl.setStage(rs.getString("job_stage"));
                ingestionLogDtl.setStatus(rs.getString("job_status"));
                ingestionLogDtl.setJobMessage(rs.getString("job_msg"));
                ingestionLogDtl.setCreated(rs.getTimestamp("created"));
                ingestionLogDtl.setCreatedBy(rs.getString("created_by"));
                // ingestionLogDtl.setLastModified(rs.getDate("last_modified"));
                // ingestionLogDtl.setUpdatedBy(rs.getString("updated_by"));
                ingestionLogDtls.add(ingestionLogDtl);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
        }
        // finally {
        // closeConnection(connection);
        // }

        return ingestionLogDtls;
    }

    // private void closeConnection(Connection con) {
    // return;
    // /*
    // * try { if(null != con) con.close(); } catch (SQLException e) {
    // * e.printStackTrace(); }
    // */
    //
    // }

    public List<PipelineStageLog> getPipelineStageLogDetailsById(
            Integer pipelineRunId) throws Exception {

        List<PipelineStageLog> pipelineStageLogDtls = new ArrayList<PipelineStageLog>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            String sQuery = ConnectionUtility.getSQlProperty("SELECT_STAGE_LOG");
            connection = ConnectionUtility.getConnection();
            preparedStatement = connection.prepareStatement(sQuery);
            preparedStatement.setInt(1, pipelineRunId);
            rs = preparedStatement.executeQuery();
            while (rs.next()) {
                PipelineStageLog pipelineStageLogDtl = new PipelineStageLog();
                pipelineStageLogDtl.setPipelineRunId(rs.getInt("pipeline_run_id"));
                pipelineStageLogDtl.setStage(rs.getString("stage"));
                pipelineStageLogDtl.setRunStartTime(rs.getTimestamp("run_start_time"));
                pipelineStageLogDtl.setRunEndTime(rs.getDate("run_end_time"));
                pipelineStageLogDtl.setStatus(rs.getString("status"));
                pipelineStageLogDtl.setMsg(rs.getString("msg"));
                pipelineStageLogDtls.add(pipelineStageLogDtl);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
        }

        return pipelineStageLogDtls;
    }

    /**
     * this method is used to list all successfully processed pipelines
     *
     * @param type
     * @return List of ProcessedPipeline
     * @throws Exception
     * @throws SQLException
     */
    public List<ProcessedPipeline> getProcessedPipelines() throws Exception {
        List<ProcessedPipeline> processedPipelineList = new ArrayList<ProcessedPipeline>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            String sQuery = ConnectionUtility.getSQlProperty("LIST_PIPELINE");
            connection = ConnectionUtility.getConnection();
            preparedStatement = connection.prepareStatement(sQuery);
            rs = preparedStatement.executeQuery();
            while (rs.next()) {
                ProcessedPipeline processedPipeline = new ProcessedPipeline();
                processedPipeline.setName(rs.getString("PIPELINE_NAME"));
                processedPipeline.setDataSet(rs.getString("OUTPUT_DATASET"));
                processedPipelineList.add(processedPipeline);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
        }
        // finally {
        // closeConnection(connection);
        // }

        return processedPipelineList;
    }

    /**
     * this method is used to list all Machine Learning pipelines
     *
     * @param type
     * @return List of MLAnalysis
     * @throws Exception
     * @throws SQLException
     */

    public List<MLAnalysis> getMLAnalysis() throws Exception {
        List<MLAnalysis> mlAnalysisList = new ArrayList<MLAnalysis>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            String sQuery = ConnectionUtility.getSQlProperty("LIST_MLPIPELINE");
            connection = ConnectionUtility.getConnection();
            preparedStatement = connection.prepareStatement(sQuery);
            rs = preparedStatement.executeQuery();
            while (rs.next()) {
                MLAnalysis mlAnalysis = new MLAnalysis();

                ProcessedPipeline trainingPipeline = new ProcessedPipeline();
                String[] trainingData = rs.getString("training").split("\\|");
                trainingPipeline.setName(trainingData[0]);
                trainingPipeline.setDataSet(trainingData[1]);
                trainingPipeline.setDataType("Training Data");

                ProcessedPipeline testingPipeline = new ProcessedPipeline();
                String[] testingData = rs.getString("testing").split("\\|");
                testingPipeline.setName(testingData[0]);
                testingPipeline.setDataSet(testingData[1]);
                testingPipeline.setDataType("Testing Data");

                mlAnalysis.setMlId(rs.getInt("ml_id"));
                mlAnalysis.setAlgorithm(rs.getString("algorithm"));
                mlAnalysis.setAccuracy(rs.getInt("accuracy"));
                mlAnalysis.setTraining(trainingPipeline);
                mlAnalysis.setTesting(testingPipeline);

                mlAnalysisList.add(mlAnalysis);

            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
        }
        // finally {
        // closeConnection(connection);
        // }

        return mlAnalysisList;

    }

    /**
     * this method is used to save ML analysis result
     *
     * @param mlAnalysis
     * @throws Exception
     * @throws SQLException
     */

    public void addMLAnalysis(MLAnalysis mlAnalysis) throws Exception {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            String sQuery = ConnectionUtility.getSQlProperty("INSERT_MLANALYSIS");
            connection = ConnectionUtility.getConnection();
            preparedStatement = connection.prepareStatement(sQuery);
            preparedStatement.setString(1,
                    mlAnalysis.getTraining().getName() + "|" + mlAnalysis.getTraining().getDataSet());
            preparedStatement.setString(2,
                    mlAnalysis.getTesting().getName() + "|" + mlAnalysis.getTesting().getDataSet());
            preparedStatement.setString(3, mlAnalysis.getAlgorithm());
            preparedStatement.setInt(4, mlAnalysis.getAccuracy());

            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtility.releaseConnectionResources(preparedStatement, connection);
        }
        // finally {
        // closeConnection(connection);
        // }

    }

    /**
     * this method is to delete MLAnalysis details
     *
     * @param id
     * @throws Exception
     */
    public void deleteMLAnalysis(Integer id) throws Exception {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            String sQuery = ConnectionUtility.getSQlProperty("DELETE_MLANALYSIS");
            connection = ConnectionUtility.getConnection();
            preparedStatement = connection.prepareStatement(sQuery);
            preparedStatement.setInt(1, id);
            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        // finally {
        // closeConnection(connection);
        // }

    }

    /**
     *
     * @param ingestionJobId
     * @return
     * @author 19217
     * @throws Exception
     * @throws SQLException
     */
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

    /**
     * method to get list of source locations of a data source.
     *
     * @return list of data source locations.
     * @throws Exception
     * @throws SQLException
     */
    public static List<String> getSourceLocations() throws Exception {

        List<String> dataSourceLocationList = new ArrayList<>();

        String sQuery = ConnectionUtility.getSQlProperty("LIST_ENTITY");

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            connection = ConnectionUtility.getConnection();
            preparedStatement = connection.prepareStatement(sQuery);
            preparedStatement.setString(1, "DataSource");
            rs = preparedStatement.executeQuery();

            while (rs.next()) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode dataSource = mapper.readTree(rs.getString("json_data"));
                JsonNode sourceNode = dataSource.get("location");

                if (sourceNode != null) {
                    String sourceLocation = sourceNode.getTextValue();
                    if (sourceLocation.endsWith("/")) {
                        sourceLocation = sourceLocation.substring(0, sourceLocation.length() - 1);
                    }
                    dataSourceLocationList.add(sourceLocation);
                }
            }

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
        }
        return dataSourceLocationList;
    }

    /**
     * method to check whether profile name already exist or not
     *
     * @return boolean values true if name is exist else return false.
     * @throws Exception
     * @throws SQLException
     */
    public static boolean getDataschemaName(String name) throws Exception {

        String sQuery = ConnectionUtility.getSQlProperty("GET_SCHMEA_NAME");

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            connection = ConnectionUtility.getConnection();
            preparedStatement = connection.prepareStatement(sQuery);
            preparedStatement.setString(1, "Dataschema");
            preparedStatement.setString(2, "Bulk");
            preparedStatement.setString(3, name);
            rs = preparedStatement.executeQuery();

            if (!rs.next()) {
                return false;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
        }
        return true;
    }
    public static boolean getBulkName(String name) throws Exception {

        String sQuery = ConnectionUtility.getSQlProperty("GET_BULK_NAME");

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            connection = ConnectionUtility.getConnection();
            preparedStatement = connection.prepareStatement(sQuery);
            preparedStatement.setString(1, name);
            rs = preparedStatement.executeQuery();

            if (!rs.next()) {
                return false;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
        }
        return true;
    }

    /**
     * Method retrieves data schema name attributed with the dataset.
     *
     * @param datasetName
     *            {@link String} name of the dataset.
     * @return {@link String} Name of the schema
     * @throws Exception
     */
    public String getSchemaName(String datasetName) throws Exception {

        Entity dataset = this.getEntityByName(datasetName);
        return retrieveSchemaForDataset(dataset);
    }

    /**
     * Method retrieves data schema name attributed with the dataset.
     *
     * @param datasetName
     *            {@link String} name of the dataset.
     * @return {@link String} Name of the schema
     */
    // TODO need to consult manohar/tuntun. below function commented by deepak
    // public String getSchemaName(int datasetId) {
    // Entity dataset = this.getEntityById("dataset", datasetId);
    // return retrieveSchemaForDataset(dataset);
    // }

    public String retrieveSchemaForDataset(Entity e) {
        String schemaName = "";
        if (e.getId() != 0) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(e.getJsonblob());
                schemaName = rootNode.get("Schema").getTextValue();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return schemaName;
    }

    /**
     * Method returns list of column names for a given dataschema
     *
     * @param schema
     *            {@link String} Name of the schema
     * @return {@link List} of columns for a given schema
     * @throws Exception
     */
    public List<String> getColumns(String schema) throws Exception {

        List<String> columns = new ArrayList<String>();

        if (schema != null && !schema.isEmpty()) {
            Entity dataschema = this.getEntityByName(schema);
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(dataschema.getJsonblob());
                JsonNode colAttrs = rootNode.path("dataAttribute");

                for (JsonNode jsonNode : colAttrs) {
                    columns.add(jsonNode.path("Name").getTextValue());
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return columns;
    }

    /**
     * this method is to delete Entity details
     *
     * @param datasetID
     *            ,datasourceID,dataschemaID,dataschedularID,destDir,schemaName
     * @throws SQLException
     * @throws IOException
     */
    public void moveToArchive(String userName, String dataSetId, String dataSourceId, String dataSchemaId,
                              String dataSchedularId, String schemaName, String destDir) throws ZeasException, SQLException, IOException {

        boolean isSuccess = false;
        System.out.println("EntityManager.moveToArchive(): " + userName + " dataSetId: " + dataSetId + " dataSchemaId: "
                + dataSchemaId + "dataSourceId:" + dataSourceId + " dataSchedularId: " + dataSchedularId
                + " schemaname: " + schemaName);
        PreparedStatement preparedStatement = null;
        PreparedStatement preparedStatement1 = null;
        PreparedStatement preparedStatement2 = null;
        Connection connection = null;
        HashMap<String, String> jsonData = new HashMap<String, String>();
        ResultSet rs = null;
        try {
            connection = ConnectionUtility.getConnection();
            connection.setAutoCommit(false);
            // getting json data of ingestion profile
            String sQuery = ConnectionUtility.getSQlProperty("GET_INGESTION_DETAILS");
            try {
                preparedStatement = connection.prepareStatement(sQuery);
                preparedStatement.setString(1, dataSetId);
                preparedStatement.setString(2, dataSourceId);
                preparedStatement.setString(3, dataSchemaId);
                preparedStatement.setString(4, dataSchedularId);
                rs = preparedStatement.executeQuery();
                while (rs.next()) {
                    jsonData.put(rs.getString("type"), rs.getString("json_data"));
                }
            } catch (SQLException ex) {

                throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION, ex.toString(),
                        "Problem in getting ingestion profile from entity table");
            } finally {
                if (rs != null) {
                    rs.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                // if (connection != null) {
                // connection.close();
                // }
            }

            // try {

            // insert col1---hm.get(DataSchema) || col2---hm.get( DataSource)
            // json data moving to schema_archive table
            String sQuery1 = ConnectionUtility.getSQlProperty("COPY_TO_ARCHIVE_TABLE");
            preparedStatement1 = connection.prepareStatement(sQuery1);
            preparedStatement1.setInt(1, Integer.parseInt(dataSchemaId));
            preparedStatement1.setString(2, schemaName);
            preparedStatement1.setString(3, (String) jsonData.get("DataSet"));
            preparedStatement1.setString(4, (String) jsonData.get("DataSource"));
            preparedStatement1.setString(5, (String) jsonData.get("DataSchema"));
            preparedStatement1.setString(6, (String) jsonData.get("DataIngestion"));
            preparedStatement1.setString(7, userName);
            preparedStatement1.executeUpdate();

            // delete all injestion profile data after moving to archive
            String sQuery2 = ConnectionUtility.getSQlProperty("DELETE_ENTITY_IDS");
            preparedStatement2 = connection.prepareStatement(sQuery2);
            preparedStatement2.setInt(1, Integer.parseInt(dataSchemaId));
            preparedStatement2.setInt(2, Integer.parseInt(dataSourceId));
            preparedStatement2.setInt(3, Integer.parseInt(dataSetId));
            preparedStatement2.setInt(4, Integer.parseInt(dataSchedularId));
            preparedStatement2.executeUpdate();
            connection.commit();
            connection.setAutoCommit(true);
            isSuccess = true;

            // moveDataToArchive(hdfsDirPath,archivePath);

        } catch (Exception e) {
            // connection.rollback();
            throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION, e.toString(),
                    "Problem in deleting ingestion profile from entity table");
        } finally {
            // try -finally block added by deepak
            try {
                if (isSuccess) {
                    ZDPDataAccessObjectImpl accessImpl = new ZDPDataAccessObjectImpl();
                    List<String> userList = new ArrayList<>();
                    userList.add(userName);
                    accessImpl.addActivitiesBatchForNewAPI(schemaName,
                            "Ingestion profile '" + schemaName + "' deleted by " + userName,
                            ZDPDaoConstant.INGESTION_ACTIVITY, ZDPDaoConstant.DELETE_ACTIVITY, userList, userName);
                    // ZDPDaoUtility.addActivities(schemaName, connection,
                    // userName,
                    // ZDPDaoConstant.INGESTION_ACTIVITY,
                    // ZDPDaoConstant.DELETE_ACTIVITY);
                }
            } finally {
                if (preparedStatement1 != null) {
                    preparedStatement1.close();
                }
                if (preparedStatement2 != null) {
                    preparedStatement2.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
            // connection.close();
        }
    }

    public void moveDataToArchive(String destDir, String dataSchemaId) throws ZeasException {
        try {
            Path hdfsDirPath = new Path(destDir);
            Path archivePath = new Path(ConfigurationReader.getProperty("ARCHIVE_DIR") + File.separator + dataSchemaId);
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", ConfigurationReader.getProperty("HDFS_FQDN"));

            /**
             * If transparent encryption is enabled on the cluster, We need to
             * specify Key Provider Uri
             */
            if (ConfigurationReader.getProperty("KEYPROVIDER_URI") != null) {
                conf.set(DataLoader.KEY_PROVIDER_URI, ConfigurationReader.getProperty("KEYPROVIDER_URI"));
            }

            FileSystem hdfs = FileSystem.get(conf);
            if (hdfs.exists(archivePath)) {
                hdfs.delete(archivePath, true);
            }
            if (hdfs.exists(hdfsDirPath)) {
                FileUtil.copy(hdfs, hdfsDirPath, hdfs, archivePath, true, conf);
                // hdfs.rename(hdfsDirPath, archivePath);
            } else {
                System.out.println("HDFS file is not exists still deleting Ingestion profile");
            }
        } catch (Exception hadoopException) {
            System.out.println(hadoopException.getMessage());
            throw new ZeasException(ZeasErrorCode.ZEAS_EXCEPTION, " ", "Error while  moving data to archive.");
        }

    }

    /**
     * Gives archived file info for admin user Fix for BugID:86
     *
     * @return gives a list of POJO object of type 'ArchivedFileInfo'
     * @throws Exception
     */
    private List<ArchivedFileInfo> getArchiveProfilesForAdminUser()
            throws Exception {

        LOGGER.debug("inside function getArchiveProfilesForAdminUser");
        String sQuery = ConnectionUtility.getSQlProperty("LIST_ARCHIVE_PROFILES");
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        List<ArchivedFileInfo> archivedFileInfoList = new ArrayList<ArchivedFileInfo>();
        try {
            sQuery = ConnectionUtility.getSQlProperty("LIST_ARCHIVE_PROFILES");
            connection = ConnectionUtility.getConnection();
            preparedStatement = connection.prepareStatement(sQuery);
            rs = preparedStatement.executeQuery();

            // Get schema id, name
            while (rs.next()) {
                ArchivedFileInfo archivedFileInfo = new ArchivedFileInfo();
                String schemaId = Integer.toString(rs.getInt("schema_id"));
                String schemaName = rs.getString("schema_name");
                archivedFileInfo.setArchivedSchemaId(schemaId);
                archivedFileInfo.setArchivedSchemaName(schemaName);
                archivedFileInfo.setPermissionLevel(UserManagementConstant.READ_WRITE_EXECUTE);
                archivedFileInfoList.add(archivedFileInfo);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            LOGGER.info("EntityManager.restoreArchivedData(): SQLException: " + e.getMessage());
            throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION, "Processing request failed.", "");
        } finally {
            ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
        }
        return archivedFileInfoList;
    }

    /**
     * Gives archived file info list Fix for BugID:86
     *
     * @param httpServletRequest
     * @return gives a list of POJO object of type 'ArchivedFileInfo'
     * @throws Exception
     */
    public List<ArchivedFileInfo> getArchiveProfiles(
            HttpServletRequest httpServletRequest) throws Exception {

        LOGGER.debug("inside function getArchiveProfiles");
        CommonUtils commonUtils = new CommonUtils();
        String accessToken = commonUtils.extractAuthTokenFromRequest(httpServletRequest);
        String userId = commonUtils.getUserNameFromToken(accessToken);
        ZDPUserAccess zdpUserAccess = new ZDPUserAccessImpl();
        Boolean isSuperUser = zdpUserAccess.isSuperUser(userId);
        String sQuery = null;
        List<ArchivedFileInfo> archivedFileInfoList = new ArrayList<ArchivedFileInfo>();
        if (isSuperUser) {
            LOGGER.debug("user is super user");
            archivedFileInfoList = getArchiveProfilesForAdminUser();
        } else {
            Map<String, Integer> userNamePermissionMap = zdpUserAccess.getUserNamePermissionMap(userId);
            /** To handle scenario where user is not part of any group */
            String userNameList = "'" + userId + "'";
            for (String username : userNamePermissionMap.keySet()) {
                userNameList = userNameList + ",'" + username + "'";
            }
            System.out.println("userNameList: " + userNameList);

            // fetch logged in User dataset permission mapping
            UserLevelPermission userLevelPermission = zdpUserAccess.getUserLevelPermission(userId);
            int userLevelDatasetPermission = userLevelPermission.getDatasetPermission();
            sQuery = "SELECT USER_NAME,schema_id, schema_name FROM schema_archive where USER_NAME in(" + userNameList
                    + ")";
            Connection connection = null;
            PreparedStatement preparedStatement = null;
            ResultSet rs = null;
            try {
                connection = ConnectionUtility.getConnection();
                preparedStatement = connection.prepareStatement(sQuery);
                rs = preparedStatement.executeQuery();

                // Get schema id, name and add it into map
                while (rs.next()) {
                    ArchivedFileInfo archivedFileInfo = new ArchivedFileInfo();
                    String schemaId = Integer.toString(rs.getInt("schema_id"));
                    String schemaName = rs.getString("schema_name");
                    String createdBy = rs.getString("USER_NAME");
                    if (createdBy.equals(userId)) {
                        // creator is requesting user
                        archivedFileInfo.setPermissionLevel(userLevelDatasetPermission);
                    } else {
                        Integer groupLevelpermission = userNamePermissionMap.get(createdBy);
                        archivedFileInfo.setPermissionLevel((groupLevelpermission & userLevelDatasetPermission));
                    }
                    archivedFileInfo.setArchivedSchemaId(schemaId);
                    archivedFileInfo.setArchivedSchemaName(schemaName);
                    archivedFileInfoList.add(archivedFileInfo);
                }
            } catch (SQLException e) {
                e.printStackTrace();
                LOGGER.info("EntityManager.restoreArchivedData(): SQLException: " + e.getMessage());
                throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION, "Processing request failed.", "");
            } finally {
                if (rs != null) {
                    rs.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
        }
        return archivedFileInfoList;
    }

    /**
     * Method returns map of schema id and name
     *
     * @return HashMap - Contains schema name and id
     * @throws ZeasException
     */
    public void restoreArchive(int schemaId, HttpServletRequest httpServletRequest) throws ZeasException {

        boolean isSuccess = false;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {

            String sQuery = ConnectionUtility.getSQlProperty("SELECT_ARCHIVE_SCHEMA");
            connection = ConnectionUtility.getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(sQuery);
            preparedStatement.setInt(1, schemaId);
            rs = preparedStatement.executeQuery();

            if (rs.next()) {
                String schemaName = rs.getString("schema_name");
                String schemaJson = rs.getString("schema_json");
                String sourceJson = rs.getString("source_json");
                String datasetJson = rs.getString("dataset_json");
                String schedularJson = rs.getString("schedular_json");
                String userName = rs.getString("user_name");

                addJsonData(schemaName, schemaJson, userName, "DataSchema", httpServletRequest);
                addJsonData((schemaName + "_Source"), sourceJson, userName, "DataSource", httpServletRequest);
                addJsonData((schemaName + "_DataSet"), datasetJson, userName, "DataSet", httpServletRequest);
                addJsonData((schemaName + "_Schedular"), schedularJson, userName, "DataIngestion", httpServletRequest);

                connection.commit();
                connection.setAutoCommit(true);
                isSuccess = true;
                // deleteArchivedData(new File(archPath));
                deleteSchemaFromArchive(schemaId);
            }

        } catch (SQLException e) {
            e.printStackTrace();
            LOGGER.info("EntityManager.restoreArchivedData(): SQLException: " + e.getMessage());
            throw new ZeasSQLException(ZeasErrorCode.SQL_EXCEPTION, "Processing Request Failed. Refer LOGS", "");
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.info("EntityManager.restoreArchivedData(): Exception: " + e.getMessage());
            throw new ZeasException(ZeasErrorCode.ZEAS_EXCEPTION,
                    "Processing Request Failed. Please check hadoop Configuration. Refer LOGS", "");
        }
        // commented as same Exception e is declared above
		/*
		 * catch (Throwable e) { e.printStackTrace(); LOGGER.info(
		 * "EntityManager.restoreArchivedData(): Throwable: " + e.getMessage());
		 * throw new ZeasException( ZeasErrorCode.ZEAS_EXCEPTION,
		 * "Processing Request Failed. Please check hadoop Configuration. Refer LOGS"
		 * , ""); }
		 */ finally {
            try {
                if (!isSuccess) {
                    // checking for connection null value
                    if (connection != null) {
                        connection.rollback();
                        connection.setAutoCommit(true);
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
            }
        }
    }

    public void restoreDataFromArchive(String schemaId, String hdfsPath) throws ZeasException {
        String archPath = ConfigurationReader.getProperty("ARCHIVE_DIR") + File.separator + schemaId;
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", ConfigurationReader.getProperty("HDFS_FQDN"));
            FileSystem hdfs = FileSystem.get(conf);
            Path hdfsDirPath = new Path(hdfsPath);
            Path archivePath = new Path(archPath);
            hdfs.mkdirs(hdfsDirPath);
            FileUtil.copy(hdfs, archivePath, hdfs, hdfsDirPath, true, conf);
            // hdfs.copyFromLocalFile(archivePath, hdfsDirPath);
        } catch (Exception hadoopException) {
            System.out.println(hadoopException.getMessage());
            throw new ZeasException(ZeasErrorCode.ZEAS_EXCEPTION, " ", "Error while  moving data from archive.");
        }
    }

    /**
     * Method for getting hdfs path from daset json (from archive data
     * information)
     *
     * @param schema
     *            id
     * @return String - hdfs path
     * @throws ZeasException
     * @throws SQLException
     */
    public String getHdfsPath(int schemaId) throws ZeasException, SQLException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {

            String sQuery = ConnectionUtility.getSQlProperty("SELECT_DATASET_JSON");
            connection = ConnectionUtility.getConnection();
            preparedStatement = connection.prepareStatement(sQuery);
            preparedStatement.setInt(1, schemaId);
            rs = preparedStatement.executeQuery();

            if (rs.next()) {
                String datasetJson = rs.getString("dataset_json");
                JSONObject jsonObj = new JSONObject(datasetJson);
                this.hdfsPath = jsonObj.getString("location");
            }

        } catch (Exception e) {
            LOGGER.info("EntityManager.getHdfsPath(): Exception: " + e.getMessage());
            throw new ZeasException(ZeasErrorCode.ZEAS_EXCEPTION, "Processing Request Failed. Refer LOGS", "");
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return hdfsPath;
    }

    /**
     * Method for adding jsons of archived profiles while restoring data
     *
     * @param schemaName
     * @param schemaJson
     * @param userName
     * @param type
     * @throws Exception
     */
    private void addJsonData(String schemaName, String schemaJson,
                             String userName, String type, HttpServletRequest httpServletRequest)
            throws Exception {

        Entity entity = new Entity();
        entity.setName(schemaName);
        entity.setType(type);
        entity.setJsonblob(schemaJson);
        entity.setCreatedBy(userName);
        entity.setUpdatedBy(userName);
        entity.setActive(true);

        addEntity(entity, httpServletRequest);

    }

    /**
     * Method for deleting schema from archive table once the data is restored
     * to hdfs location
     *
     * @param schemaId
     * @throws Exception
     */
    private void deleteSchemaFromArchive(int schemaId) throws Exception {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {

            String sQuery = ConnectionUtility.getSQlProperty("DELETE_ARCHIVE_SCHEMA");
            connection = ConnectionUtility.getConnection();
            preparedStatement = connection.prepareStatement(sQuery);
            preparedStatement.setInt(1, schemaId);
            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            LOGGER.info("EntityManager.deleteSchemaFromArchive(): SQLException: " + e.getMessage());
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Method for deleting the data from local archived folder once restoring of
     * data is done
     *
     * @param dir
     *            - folder to be deleted
     * @throws ZeasException
     */
    private boolean deleteArchivedData(File dir) throws ZeasException {

        boolean deletedStatus = false;

        try {

            if (dir.isDirectory()) {
                String[] children = dir.list();
                for (int i = 0; i < children.length; i++) {
                    boolean success = deleteArchivedData(new File(dir, children[i]));
                    if (!success) {
                        return false;
                    }
                }
            }

            deletedStatus = dir.delete();
        } catch (Exception e) {
            LOGGER.info("EntityManager.deleteSchemaFromArchive(): Exception: " + e.getMessage());
        }

        return deletedStatus;
    }

    /**
     * Getting HDFS path from detaset name
     *
     * @param dataset
     * @return
     * @throws Exception
     */
    public String getdatasetPath(String dataset) throws Exception {
        String datasetPath = "";
        try {
            EntityManager em = new EntityManager();
            ObjectMapper mapper = new ObjectMapper();
            Entity ds = em.getEntityByName(dataset);
            JsonNode rootNode;
            rootNode = mapper.readTree(ds.getJsonblob());
            datasetPath = (rootNode.get("location")).getTextValue();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return datasetPath;
    }

    public Map<Long, String> getProfileRunStatus(String userId) throws Exception {

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
        } finally {
            ConnectionUtility.releaseConnectionResources(rs, preparedStatement, connection);
        }

        return runStatus;
    }

    /**
     * check for same project name exist with given user.
     *
     * @param projectName
     * @param userName
     * @return
     */
    public Boolean isProjectExist(String projectName, String userName) {

        // it is used to say project already exist with this name if value is
        // true.
        Boolean isProjectExist = true;
        try {
            ZDPDataAccessObject dao = new ZDPDataAccessObjectImpl();
            isProjectExist = dao.isProjectExist(projectName, userName);
            LOGGER.info("isProjectExist=" + isProjectExist);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
        }
        LOGGER.info("validateProjectName finish with isProjectExist=" + isProjectExist);

        return isProjectExist;
    }

    /**
     * Method to fetch dataset path details info.Refers config.properties file
     * to find details. It carries info like whether Transparent encryption is
     * enabled or not, what is rootPath for dataset, and encryptionZone path
     *
     * @return path details in {@link DatasetPathDetails}
     */
    public DatasetPathDetails getDatasetPathDetails() {
        DatasetPathDetails pathInfo = new DatasetPathDetails();
        pathInfo.setEncryptionAvailable(
                ConfigurationReader.getProperty("TRANSPARENT_ENCRYPTION_ENABLED").equalsIgnoreCase("true"));
        pathInfo.setDatasetRootPath(ConfigurationReader.getProperty("DATASET_ROOT_PATH"));
        pathInfo.setEncryptionZonePath(ConfigurationReader.getProperty("ENCRYPTION_ZONE_PATH"));
        return pathInfo;
    }

}