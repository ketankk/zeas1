package com.itc.zeas.datagovernance;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import javax.xml.bind.DatatypeConverter;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasException;
import com.itc.zeas.utility.utility.ConfigurationReader;

/**
 * @author Ketan on 5/27/2017.
 */
public class AtlasHelper {

	private static final Logger LOG = Logger.getLogger(AtlasHelper.class);
	static String BASE_URL = "http://zlab-physrv2:21000";
	String password = "admin";
	String username = "admin";
	String DB_NAME = "zeas";

	AtlasHelper() {

		BASE_URL = ConfigurationReader.getProperty("ATLAS_HOST");
		username = ConfigurationReader.getProperty("ATLAS_USERNAME");
		password = ConfigurationReader.getProperty("ATLAS_PASSWORD");
		DB_NAME = ConfigurationReader.getProperty("ATLAS_ZEAS_HIVE_DB");

	}

	private String getResponse(URL url) throws ZeasException {
		try {

			String authString = username + ":" + password;

			String encoded = DatatypeConverter.printBase64Binary(authString.getBytes());

			HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
			urlConnection.setRequestProperty("Authorization", "Basic " + encoded);
			urlConnection.setRequestMethod("GET");
			InputStream is = urlConnection.getInputStream();
			InputStreamReader isr = new InputStreamReader(is);

			int numCharsRead;
			char[] charArray = new char[111024];
			StringBuffer sb = new StringBuffer();
			while ((numCharsRead = isr.read(charArray)) > 0) {
				sb.append(charArray, 0, numCharsRead);
			}
			String result = sb.toString();
			LOG.info("Response from api " + url.toString() + " is " + result);
			return result;

		} catch (Exception e) {
			LOG.error("Exception while accessing Atlas api " + e.getMessage());
			throw new ZeasException(ZeasErrorCode.SERVICE_ISSUE,
					"Exception while accessing Atlas api " + e.getMessage());
		}
	}

	private int postRequest(URL url, String data) throws ZeasException {
		try {
			String usernameAndPassword = username + ":" + password;
			String encoded = DatatypeConverter.printBase64Binary(usernameAndPassword.getBytes());

			HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
			urlConnection.setRequestProperty("Authorization", "Basic " + encoded);
			urlConnection.setRequestMethod("POST");
			urlConnection.setRequestProperty("Content-Type", "application/json");
			urlConnection.setDoOutput(true);
			urlConnection.setDoInput(true);

			OutputStream out = urlConnection.getOutputStream();

			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
			bw.write(data);
			bw.flush();
			bw.close();
			out.close();

			urlConnection.connect();
			int res = urlConnection.getResponseCode();
			return res;

		} catch (Exception e) {
			LOG.error("Exception while adding " + data + " to " + " URL:" + url.toString() + e.getMessage());
			throw new ZeasException();
		}
	}

	public String getGraphData(String guid) throws Exception {

		String graphInApi = "http://zlab-physrv2:21000/api/atlas/lineage/" + guid + "/inputs/graph";
		String graphoutApi = "http://zlab-physrv2:21000/api/atlas/lineage/" + guid + "/outputs/graph";
		URL url = new URL(graphInApi);
		String resData = getResponse(url);
		JsonParser parser = new JsonParser();
		JsonArray dbList = parser.parse(resData).getAsJsonObject().getAsJsonArray("edges");
		for (JsonElement ele : dbList) {
			String dbName = ele.getAsJsonObject().get("name").getAsString();
			if (dbName.equals(DB_NAME)) {
				String dbId = ele.getAsJsonObject().get("$id$").getAsJsonObject().get("id").getAsString();
				return dbId;

			}

		}
		throw new Exception("Entity with guid: " + guid + " not found ");

	}

	public JsonElement getLineageInput(String guid) throws Exception {

		String graphInApi = BASE_URL + "/api/atlas/lineage/" + guid + "/inputs/graph";
		URL url = new URL(graphInApi);
		String resData = getResponse(url);
		JsonParser parser = new JsonParser();
		JsonElement edges = parser.parse(resData).getAsJsonObject().get("results").getAsJsonObject().get("values");
		return edges;

	}

	JsonElement getEntityDetails(String guid) throws ZeasException {
		String api = BASE_URL + "/api/atlas/entities/" + guid;
		URL url;
		try {
			url = new URL(api);
		} catch (MalformedURLException e) {
			throw new ZeasException(ZeasErrorCode.URL_NOT_PROPER, api + " Url is not correct ");

		}

		String res = getResponse(url);
		JsonParser parser = new JsonParser();
		JsonElement json = parser.parse(res).getAsJsonObject().get("definition");
		return json;

	}

	public JsonElement getLineageOutput(String guid) throws Exception {

		String graphoutApi = BASE_URL + "/api/atlas/lineage/" + guid + "/outputs/graph";
		URL url = new URL(graphoutApi);
		String resData = getResponse(url);
		JsonParser parser = new JsonParser();
		JsonElement edges = parser.parse(resData).getAsJsonObject().get("results").getAsJsonObject().get("values");// .getAsJsonObject().get("edges");
		return edges;

	}

	/**
	 * Get list of hive tables in a given database guid Map contains guid as key
	 * and value as table name
	 *
	 * @param zeasdbId
	 * @return
	 * @throws ZeasException
	 * @throws IOException
	 */
	private List<AtlasEntity> listOFHiveTablesbyDbid(String zeasdbId) throws ZeasException, IOException {
		JsonArray tableList = listOFHiveTables();
		LOG.info("Getting list of hive tables for database with GUID as " + tableList);
		List<AtlasEntity> entityList = new ArrayList<>();
		for (JsonElement element : tableList) {

			String dbId = element.getAsJsonObject().get("db").getAsJsonObject().get("id").getAsString();
			String state = element.getAsJsonObject().get("$id$").getAsJsonObject().get("state").getAsString();

			String tableName = element.getAsJsonObject().get("name").getAsString();
			String tableguid = element.getAsJsonObject().get("$id$").getAsJsonObject().get("id").getAsString();
			String owner = element.getAsJsonObject().get("owner").getAsString();
			JsonElement desc = element.getAsJsonObject().get("description");
			JsonElement traits = element.getAsJsonObject().get("$traits$");
			// if list contains any trait/tag

			if (zeasdbId.equals(dbId) && state.equals("ACTIVE")) {
				AtlasEntity atlasEntity = new AtlasEntity();
				atlasEntity.setEntityGuid(tableguid);
				atlasEntity.setEntityName(tableName);
				atlasEntity.setEntityOwner(owner);
				// set description as empty string
				atlasEntity.setEntityDesc("");

				if (traits != null) {
					List<String> tagList = new ArrayList<>();
					for (Entry<String, JsonElement> ele : traits.getAsJsonObject().entrySet()) {
						String key = ele.getKey();
						JsonElement elem = ele.getValue();
						String tagName = elem.getAsJsonObject().get("$typeName$").getAsString();
						tagList.add(tagName);

					}
					atlasEntity.setTagList(tagList);
				}

				// if description is not null then update description with that
				if (!desc.isJsonNull())
					atlasEntity.setEntityDesc(desc.getAsString());
				entityList.add(atlasEntity);

			}
		}
		LOG.info("List of hive tables is " + entityList);
		return entityList;
	}

	AtlasEntity constructEntity(JsonElement element) {
		System.out.println("eklem" + element);
		JsonElement elem2 = element.getAsJsonObject().get("values");
		if (elem2 != null) {
			JsonElement name = elem2.getAsJsonObject().get("name");
			JsonElement entityguid = element.getAsJsonObject().get("id").getAsJsonObject().get("id");
			JsonElement owner = elem2.getAsJsonObject().get("owner");

			JsonElement desc = elem2.getAsJsonObject().get("description");
			JsonArray traits = element.getAsJsonObject().get("traitNames").getAsJsonArray();
			// if list contains any trait/tag
			AtlasEntity atlasEntity = new AtlasEntity();
			if (owner != null && !owner.isJsonNull()) {
				atlasEntity.setEntityOwner(owner.getAsString());

			}
			if (!entityguid.isJsonNull()) {
				atlasEntity.setEntityGuid(entityguid.getAsString());

			}
			if (name != null && !name.isJsonNull())
				atlasEntity.setEntityName(name.getAsString());
			// set description as empty string
			atlasEntity.setEntityDesc("");
			if (!traits.isJsonNull() && traits != null && traits.size() > 0) {
				List<String> tagList = new ArrayList<>();
				for (JsonElement ele : traits) {
					String traitName = ele.getAsString();
					System.out.println("list:" + traitName);
					tagList.add(traitName);
				}
				atlasEntity.setTagList(tagList);
			}

			// if description is not null then update description with that
			if (desc != null && !desc.isJsonNull())
				atlasEntity.setEntityDesc(desc.getAsString());
			return atlasEntity;
		}
		// remove this TODO
		return null;

	}

	private JsonArray listOFHiveTables() throws IOException, ZeasException {
		String api = BASE_URL + ConfigurationReader.getProperty("ATLAS_LIST_TABLES_API");

		URL url = new URL(api);

		String resData = getResponse(url);
		JsonParser parser = new JsonParser();
		JsonArray tableList = parser.parse(resData).getAsJsonObject().getAsJsonArray("results");
		return tableList;

	}

	/**
	 * method to get a map of guid as key and its name as value
	 *
	 * @return
	 * @throws ZeasException
	 * @throws MalformedURLException
	 */
	private List<AtlasEntity> gethiveDbList() throws ZeasException, MalformedURLException {

		String dbApi = BASE_URL + ConfigurationReader.getProperty("ATLAS_LIST_DB_API");

		URL url = new URL(dbApi);
		String resData = getResponse(url);
		JsonParser parser = new JsonParser();

		JsonArray dbList = parser.parse(resData).getAsJsonObject().getAsJsonArray("results");
		List<AtlasEntity> hivedbs = new ArrayList<>();
		for (JsonElement ele : dbList) {
			String dbName = ele.getAsJsonObject().get("name").getAsString();
			String dbguId = ele.getAsJsonObject().get("$id$").getAsJsonObject().get("id").getAsString();
			String state = ele.getAsJsonObject().get("$id$").getAsJsonObject().get("state").getAsString();
			// only conside if database is active
			if (state.equals("ACTIVE")) {
				AtlasEntity atlasEntity = new AtlasEntity();
				atlasEntity.setEntityName(dbName);
				atlasEntity.setEntityGuid(dbguId);
				hivedbs.add(atlasEntity);
			}

		}
		return hivedbs;

	}

	/**
	 * method to get the guid of any DB(currently zeas db in hive) which will we
	 * used for getting table list
	 *
	 * @return
	 * @throws Exception
	 */
	private String getZeasDbId() throws Exception {

		List<AtlasEntity> dbList = gethiveDbList();

		for (AtlasEntity db : dbList) {

			if (db.getEntityName().equals(DB_NAME)) {
				String dbguid = db.getEntityGuid();
				LOG.info("Zeas Db GUID is " + dbguid);
				return dbguid;

			}

		}
		throw new ZeasException(ZeasErrorCode.ENTITY_DOESNOT_EXIST, "Db not found" + DB_NAME);

	}

	/**
	 * method gets guid of zeas db and based on that gets list of hive tables in
	 * that
	 *
	 * @return
	 * @throws Exception
	 */
	List<AtlasEntity> getHiveDbTableListMap() throws Exception {
		String zeasguId = getZeasDbId();
		List<AtlasEntity> list = listOFHiveTablesbyDbid(zeasguId);
		LOG.info("Db with guid " + zeasguId + " contains these tables " + list);
		return list;
	}

	List<String> getTypeList(String type) throws ZeasException, MalformedURLException {
		String api = BASE_URL + ConfigurationReader.getProperty("ATLAS_LIST_TYPES") + "?type=" + type;

		URL url = new URL(api);
		String resData = getResponse(url);
		JsonParser parser = new JsonParser();
		Type listType = new TypeToken<List<String>>() {
		}.getType();
		JsonArray types = parser.parse(resData).getAsJsonObject().getAsJsonArray("results");
		return new Gson().fromJson(types, listType);
	}

	JsonNode getEntityAudit(String guid) throws ZeasException, IOException {

		String api = BASE_URL + "/api/atlas/entities/" + guid + "/audit";
		URL url = new URL(api);
		String response = getResponse(url);

		ObjectMapper mapper = new ObjectMapper();
		JsonNode jsonObj = mapper.readValue(response, JsonNode.class);
		LOG.info("Audit for guid:" + guid + " is " + jsonObj);
		return jsonObj;
	}

	JsonNode getSchemaJson(String guid) throws ZeasException, IOException {

		String api = BASE_URL + "/api/atlas/lineage/" + guid + "/schema";
		URL url = new URL(api);
		String response = getResponse(url);
		ObjectMapper mapper = new ObjectMapper();
		JsonNode jsonObj = mapper.readValue(response, JsonNode.class);
		LOG.info("Schema for guid:" + guid + " is " + jsonObj);

		return jsonObj;

	}

	/**
	 * a method to create tag in atlas it gets request
	 * 
	 * @param json
	 * @return
	 * @throws ZeasException
	 * @throws MalformedURLException
	 */
	int createTag(String data) throws ZeasException {
		String api = BASE_URL + ConfigurationReader.getProperty("ATLAS_LIST_TYPES");

		URL url;
		try {
			url = new URL(api);
			return postRequest(url, data);

		} catch (MalformedURLException e) {
			LOG.error(api + " is not valid url");
			throw new ZeasException(ZeasErrorCode.URL_NOT_PROPER, api + " Url is not correct ");
		}

	}

	public void addTagToEntity(String guid, String tagData) throws ZeasException {
		String api = BASE_URL + "/api/atlas/entities/" + guid + "/traits";
		try {
			URL url = new URL(api);
			postRequest(url, tagData);

		} catch (MalformedURLException e) {
			LOG.error(api + " is not valid url");
			throw new ZeasException(ZeasErrorCode.URL_NOT_PROPER, api + " is not valid url");
		}

	}

	/**
	 * method which takes guid and tagName and assigns tag to guid
	 * 
	 * @param guid
	 * @param data
	 * @return
	 * @throws ZeasException
	 */
	public int assignTag(String guid, String data) throws ZeasException {

		String api = BASE_URL + "/api/atlas/entities/" + guid + "/traits";
		return postRequest(getUrl(api), data);

	}

	/**
	 * this method will get the list of guids, which has got this tag assigned
	 * 
	 * @param tagName
	 * @return
	 * @throws ZeasException
	 */
	public List<String> getGuidbyTag(String tagName) throws ZeasException {
		LOG.info("Getting list of guids with tagname:" + tagName);
		String api = BASE_URL + "/api/atlas/discovery/search/dsl?query=%60" + tagName + "%60";

		String res = getResponse(getUrl(api));

		JsonParser parser = new JsonParser();

		JsonArray guids = parser.parse(res).getAsJsonObject().getAsJsonArray("results");
		List<String> guidList = new ArrayList<>();
		for (JsonElement ele : guids) {
			String guid = ele.getAsJsonObject().get("instanceInfo").getAsJsonObject().get("guid").getAsString();
			guidList.add(guid);

		}
		return guidList;

	}

	/**
	 * method returns th array of entities returned by text search of query
	 * 
	 * @param query
	 * @return
	 * @throws ZeasException
	 */
	public List<String> textSearch(String query) throws ZeasException {
		String api = BASE_URL + ConfigurationReader.getProperty("ATLAS_TEXT_SEARCH") + query;
		String res = getResponse(getUrl(api));
		JsonParser parser = new JsonParser();
		JsonArray jsonArr = parser.parse(res).getAsJsonObject().get("results").getAsJsonArray();

		List<String> guids = new ArrayList<>();
		for (JsonElement element : jsonArr) {
			String guid = element.getAsJsonObject().get("guid").getAsString();
			guids.add(guid);

		}
		return guids;

	}

	/**
	 * method to convert string url to URL object and throws exception if url is
	 * not proper
	 * 
	 * @param api
	 * @return
	 * @throws ZeasException
	 */

	private URL getUrl(String api) throws ZeasException {
		try {
			return new URL(api);
		} catch (MalformedURLException e) {
			LOG.error(api + " is not valid url");
			throw new ZeasException(ZeasErrorCode.URL_NOT_PROPER, api + " url is not proper");
		}

	}

	public int removeTag(String guid, String tagName) throws ZeasException {

		String api = BASE_URL + "/api/atlas/entities/" + guid + "/traits/" + tagName;
		try {
			URL url = new URL(api);
			return deleteRequest(url, tagName);
		} catch (MalformedURLException e) {
			throw new ZeasException(ZeasErrorCode.URL_NOT_PROPER, api + " url is not proper");
		}
	}

	private int deleteRequest(URL url, String tagName) throws ZeasException {
		try {
			String usernameAndPassword = username + ":" + password;
			String encoded = DatatypeConverter.printBase64Binary(usernameAndPassword.getBytes());

			HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
			urlConnection.setRequestProperty("Authorization", "Basic " + encoded);
			urlConnection.setRequestMethod("DELETE");
			urlConnection.setRequestProperty("Content-Type", "application/json");
			urlConnection.setDoOutput(true);
			urlConnection.setDoInput(true);
			urlConnection.connect();
			int res = urlConnection.getResponseCode();
			return res;

		} catch (Exception e) {
			LOG.error("Exception while adding " + "data" + " to " + " URL:" + url.toString() + e.getMessage());
			throw new ZeasException();
		}
	}

	public static void main(String[] args) throws Exception {
		String guid = "05dec44b-c4c4-44ea-9564-7acd8c0cf015";// 3137'
		String guid2 = "c4dd08ee-0deb-4fc9-b9f3-6cc6a05701e0";// 3212
		// System.out.println(new AtlasHelper().getHiveDbListMap());
		// System.out.println(new AtlasHelper().put("CLASS"));
		String url = BASE_URL + "/api/atlas/types?type=CLASS";

		String str = "{\n" + "  \"enumTypes\": [\n" + "    \n" + "  ],\n" + "  \"traitTypes\": [\n" + "    {\n"
				+ "      \"attributeDefinitions\": [\n" + "        \n" + "      ],\n"
				+ "      \"typeName\": \"test11\",\n" + "      \"typeDescription\": \"\",\n"
				+ "      \"superTypes\": [\n" + "        \n" + "      ],\n"
				+ "      \"hierarchicalMetaTypeName\": \"org.apache.atlas.typesystem.types.TraitType\"\n" + "    }\n"
				+ "  ],\n" + "  \"structTypes\": [\n" + "    \n" + "  ],\n" + "  \"classTypes\": [\n" + "    \n"
				+ "  ]\n" + "}";
		System.out.println("guids" + new AtlasHelper().textSearch("name"));

	}

}
