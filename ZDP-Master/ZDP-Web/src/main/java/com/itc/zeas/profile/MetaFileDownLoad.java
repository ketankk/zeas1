package com.itc.zeas.profile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.itc.zeas.validation.rule.JSONDataParser;
import com.itc.zeas.profile.model.Entity;

@Controller
@RequestMapping("/file-download")
public class MetaFileDownLoad {
	@RequestMapping(value = "/getSchemaFileInTextFormat/{schemaName}/{type}", method = RequestMethod.GET)
	public void getSchemaFileInTextFormat(@PathVariable("schemaName") String schemaName,@PathVariable("type") String type, HttpServletRequest request,
			HttpServletResponse response) throws IOException, SQLException {
		try{
		
			EntityManager entityManager = new EntityManager();
			Entity entity = new Entity();
			
			if(type.equalsIgnoreCase("Bulk")){
				entity = entityManager.getBulkEntityByName(schemaName);
			}	
			else{
			entity = entityManager.getEntityByName(schemaName);
			}
		JSONDataParser dataTypeparser = new JSONDataParser();
		Map<String, String> columnNameAndDataType = dataTypeparser.JsonParser(entity.getJsonblob());

		// Prepare a file object with file to return
		String filename = schemaName + "_schemaFile.txt";
		File file = new File(filename);
		if (file.exists()) {
			file.delete();
		}
		file.createNewFile();
		FileWriter fileWriter = null;
		try {
			fileWriter = new FileWriter(file);
			fileWriter.write("Field_Name" + " | " + "Field_dataType" + ",");
			for (Map.Entry<String, String> entry : columnNameAndDataType.entrySet()) {
				fileWriter.write(entry.getKey() + " | " + entry.getValue() + ",");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileWriter != null)
					fileWriter.close();
			} catch (IOException e) {
			}
		}
		ServletContext context = request.getSession().getServletContext();
		FileInputStream inputStream = new FileInputStream(file);
		response.setContentType(context.getMimeType(file.getAbsolutePath()));
		response.setContentLength((int) file.length());
		String headerKey = "Content-Disposition";
		String headerValue = String.format("attachment; filename=\"%s\"", file.getName());
		response.setHeader(headerKey, headerValue);

		// get output stream of the response
		OutputStream outStream = response.getOutputStream();

		byte[] buffer = new byte[(int) file.length()];
		int bytesRead = -1;

		// write bytes read from the input stream into the output stream
		while ((bytesRead = inputStream.read(buffer)) != -1) {
			outStream.write(buffer, 0, bytesRead);
		}

		inputStream.close();
		outStream.close();
		file.delete();
		}catch(Exception e){
			
		}
	}
}