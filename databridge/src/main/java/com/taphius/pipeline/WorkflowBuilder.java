package com.taphius.pipeline;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

public class WorkflowBuilder {

	public static final String FILE_TYPE = "file";

	public static final String RDBMS_TYPE = "rdbms";

	public static void main(String[] args) {
		/*
		 * WorkflowBuilder wf = new WorkflowBuilder();
		 * 
		 * Document oozieWorkFlowTemplate =
		 * wf.getOozieWorkFlowTemplateForBulkFileIngestion("java-action",
		 * "java-action", "myflow", "rdbms", "/userzeas/");
		 * wf.saveWorkFlowXML(oozieWorkFlowTemplate, "D:\\workflowFile.xml");
		 * 
		 * Document oozieWorkFlowSQLTemplate =
		 * wf.getOozieWorkFlowTemplateForSQLBulkIngestion("sqoop", "sqoop",
		 * "myflow", "rdbms"); wf.saveWorkFlowXML(oozieWorkFlowSQLTemplate,
		 * "D:\\workflowsql.xml");
		 */
		String args3 = ";/user/zeas/admin/CsvTesting3//cleansed;/tmp/zeas/p_3593-3/1496149459193/3594-3;0;0";
		System.out.println(args3.indexOf(';', 1));
		String start = args3.substring(1, args3.indexOf(';', 1));
		System.out.println(start);
		args3 = start + args3;
		System.out.println(args3);

	}

	public Document getOozieWorkFlowTemplate(String actionName, String scriptName, String workflowname, String type) {

		Element rootNode = null;
		Document doc = null;
		Namespace oozieNameSpace = Namespace.getNamespace("uri:oozie:workflow:0.4");
		Namespace oozieShellNameSpace = Namespace.getNamespace("uri:oozie:shell-action:0.1");
		Namespace hiveNamespace = Namespace.getNamespace("uri:oozie:hive-action:0.2");

		try {
			SAXBuilder builder = new SAXBuilder();
			InputStream is = this.getClass().getClassLoader().getResourceAsStream("oozie-workflow-template.xml");
			doc = (Document) builder.build(is);
			rootNode = doc.getRootElement();

			rootNode.getAttribute("name").setValue(workflowname);
			rootNode.getChild("start", Namespace.getNamespace("uri:oozie:workflow:0.4")).getAttribute("to")
					.setValue(actionName);
			List<Element> childList = rootNode.getChildren();

			for (Element element : childList) {
				if (element.getName().equalsIgnoreCase("action")) {
					element.getAttribute("name").setValue(actionName);

					Element shellElement = new Element("shell");
					shellElement.setNamespace(oozieShellNameSpace);

					createElement("job-tracker", "${jobTracker}", oozieShellNameSpace, shellElement);
					createElement("name-node", "${nameNode}", oozieShellNameSpace, shellElement);

					Element configElement = new Element("configuration", oozieShellNameSpace);
					createPropertyElements("mapred.job.queue.name", "${queueName}", oozieShellNameSpace, configElement);

					shellElement.addContent(configElement);

					element.setContent(shellElement);
					createElement("exec", "${scriptName}", oozieShellNameSpace, shellElement);
					createElement("env-var", "HADOOP_USER_NAME=${wf:user()}", oozieShellNameSpace, shellElement);
					createElement("file", "${scriptPath}", oozieShellNameSpace, shellElement);

					if (type.equalsIgnoreCase(FILE_TYPE)) {
						createElement("file", "${fileschedulerJar}", oozieShellNameSpace, shellElement);
						createElement("archive", "${fileschedulerJar}", oozieShellNameSpace, shellElement);
						createElement("archive", "${scriptPath}", oozieShellNameSpace, shellElement);
					}

					Element okElement = new Element("ok");
					okElement.setNamespace(oozieNameSpace);
					okElement.setAttribute("to", "hive-action");
					element.addContent(okElement);

					Element errorElement = new Element("error");
					errorElement.setNamespace(oozieNameSpace);
					errorElement.setAttribute("to", "fail");
					element.addContent(errorElement);

				}

			}
			Element parent = new Element("action");
			parent.setNamespace(oozieNameSpace);
			parent.setAttribute("name", "hive-action");
			Element hiveElement = new Element("hive").setNamespace(hiveNamespace);
			createElement("job-tracker", "${jobTracker}", hiveNamespace, hiveElement);
			createElement("name-node", "${nameNode}", hiveNamespace, hiveElement);
			createElement("job-xml", "hive-site.xml", hiveNamespace, hiveElement);

			Element configElement = new Element("configuration", hiveNamespace);
			createPropertyElements("mapred.job.queue.name", "${queueName}", hiveNamespace, configElement);
			createPropertyElements("oozie.hive.defaults", "${nameNode}${hive_site_xml}", hiveNamespace, configElement);
			hiveElement.addContent(configElement);

			createElement("script", "${hiveScript}", hiveNamespace, hiveElement);

			parent.addContent(hiveElement);
			rootNode.addContent(4, parent);
			doc.setContent(rootNode);

			Element okElement = new Element("ok");
			okElement.setNamespace(oozieNameSpace);
			okElement.setAttribute("to", "end");
			parent.addContent(okElement);

			Element errorElement = new Element("error");
			errorElement.setNamespace(oozieNameSpace);
			errorElement.setAttribute("to", "fail");
			parent.addContent(errorElement);

			// rootNode.

			/*
			 * for (Element element : childList) { if
			 * (element.getName().equalsIgnoreCase("action")) {
			 * element.getAttribute("name").setValue(actionName);
			 * 
			 * if (!actionName.equalsIgnoreCase("mapreduce")) {
			 * 
			 * Element shellElement = new Element("shell");
			 * shellElement.setNamespace(oozieShellNameSpace);
			 * 
			 * createElement("job-tracker","${jobTracker}",oozieShellNameSpace,
			 * shellElement);
			 * createElement("name-node","${nameNode}",oozieShellNameSpace,
			 * shellElement);
			 * createElement("exec",scriptName,oozieShellNameSpace,shellElement)
			 * ;
			 * createElement("file",scriptName,oozieShellNameSpace,shellElement)
			 * ;
			 * 
			 * element.setContent(shellElement); } else{ Element mapElement =
			 * new Element("map-reduce");
			 * mapElement.setNamespace(oozieNameSpace);
			 * createElement("job-tracker","${jobTracker}",oozieNameSpace,
			 * mapElement);
			 * createElement("name-node","${nameNode}",oozieNameSpace,mapElement
			 * );
			 * 
			 * Element configElement = new Element("configuration",
			 * oozieNameSpace);
			 * 
			 * createPropertyElements("mapred.input.dir","${inputDir}",
			 * oozieNameSpace, configElement);
			 * createPropertyElements("mapred.output.dir","${outputDir}",
			 * oozieNameSpace, configElement);
			 * createPropertyElements("mapred.mapper.new-api","true",
			 * oozieNameSpace, configElement);
			 * createPropertyElements("mapred.reducer.new-api","true",
			 * oozieNameSpace, configElement);
			 * createPropertyElements("mapreduce.map.class","IngestionMapper",
			 * oozieNameSpace, configElement);
			 * createPropertyElements("mapreduce.reduce.class",
			 * "IngestionReducer",oozieNameSpace, configElement);
			 * createPropertyElements("mapred.output.key.class",
			 * "org.apache.hadoop.io.Text",oozieNameSpace, configElement);
			 * createPropertyElements("mapred.output.value.class",
			 * "org.apache.hadoop.io.Text",oozieNameSpace, configElement);
			 * 
			 * mapElement.addContent(configElement);
			 * 
			 * element.setContent(mapElement); } } }
			 */

		} catch (IOException io) {
			io.printStackTrace();
		} catch (JDOMException e) {
			e.printStackTrace();
		}
		return doc;
	}

	public Document getOozieWorkFlowTemplateForBulkFileIngestion(String actionName, String scriptName,
			String workflowname, String type, String HDFS_PATH) {

		Element rootNode = null;
		Document doc = null;
		Namespace oozieNameSpace = Namespace.getNamespace("uri:oozie:workflow:0.4");
		Namespace hiveNamespace = Namespace.getNamespace("uri:oozie:hive-action:0.2");

		try {
			SAXBuilder builder = new SAXBuilder();
			InputStream is = this.getClass().getClassLoader().getResourceAsStream("oozie-workflow-file-template.xml");
			doc = (Document) builder.build(is);
			rootNode = doc.getRootElement();

			rootNode.getAttribute("name").setValue(workflowname);

			rootNode.getChild("start", Namespace.getNamespace("uri:oozie:workflow:0.4")).getAttribute("to")
					.setValue(actionName);
			List<Element> childList = rootNode.getChildren();

			for (Element element : childList) {
				if (element.getName().equalsIgnoreCase("action")) {
					element.getAttribute("name").setValue(actionName);
					Element javaElement = new Element("java");
					javaElement.setNamespace(oozieNameSpace);
					createElement("job-tracker", "${jobTracker}", oozieNameSpace, javaElement);
					createElement("name-node", "${nameNode}", oozieNameSpace, javaElement);
					createElement("main-class", "com.taphius.validation.mr.DataIngestionControler", oozieNameSpace,
							javaElement);
					createElement("arg", "${arg0}", oozieNameSpace, javaElement);
					createElement("arg", "${arg1}", oozieNameSpace, javaElement);
					createElement("arg", "${arg2}", oozieNameSpace, javaElement);
					createElement("arg", "${arg3}", oozieNameSpace, javaElement);
					createElement("arg", "${arg4}", oozieNameSpace, javaElement);
					createElement("arg", "${arg5}", oozieNameSpace, javaElement);
					createElement("arg", "${arg6}", oozieNameSpace, javaElement);
					createElement("arg", "${arg7}", oozieNameSpace, javaElement);
					createElement("arg", "${arg8}", oozieNameSpace, javaElement);
					createElement("arg", "${arg9}", oozieNameSpace, javaElement);
					createElement("arg", "${arg10}", oozieNameSpace, javaElement);
					createElement("file", "${nameNode}" + HDFS_PATH + "/poi-3.9.jar#poi-3.9.jar", oozieNameSpace,
							javaElement);
					createElement("file", "${nameNode}" + HDFS_PATH + "/poi-3.9.jar#poi-3.9.jar", oozieNameSpace,
							javaElement);
					createElement("file", "${nameNode}" + HDFS_PATH + "/poi-3.9.jar#poi-3.9.jar", oozieNameSpace,
							javaElement);
					createElement("file", "${nameNode}" + HDFS_PATH + "/poi-3.9.jar#poi-3.9.jar", oozieNameSpace,
							javaElement);

					createElement("archive", "${nameNode}" + HDFS_PATH + "/poi-3.9.jar#poi-3.9.jar", oozieNameSpace,
							javaElement);
					createElement("archive", "${nameNode}" + HDFS_PATH + "/poi-3.9.jar#poi-3.9.jar", oozieNameSpace,
							javaElement);
					createElement("archive", "${nameNode}" + HDFS_PATH + "/poi-3.9.jar#poi-3.9.jar", oozieNameSpace,
							javaElement);
					createElement("archive", "${nameNode}" + HDFS_PATH + "/poi-3.9.jar#poi-3.9.jar", oozieNameSpace,
							javaElement);

					element.setContent(javaElement);

					Element okElement = new Element("ok");
					okElement.setNamespace(oozieNameSpace);
					okElement.setAttribute("to", "hive-action");
					element.addContent(okElement);

					Element errorElement = new Element("error");
					errorElement.setNamespace(oozieNameSpace);
					errorElement.setAttribute("to", "fail");
					element.addContent(errorElement);
				}

			}
			Element parent = new Element("action");
			parent.setNamespace(oozieNameSpace);
			parent.setAttribute("name", "hive-action");
			Element hiveElement = new Element("hive").setNamespace(hiveNamespace);
			createElement("job-tracker", "${jobTracker}", hiveNamespace, hiveElement);
			createElement("name-node", "${nameNode}", hiveNamespace, hiveElement);
			createElement("job-xml", "hive-site.xml", hiveNamespace, hiveElement);
			Element configElement = new Element("configuration", hiveNamespace);
			createPropertyElements("mapred.job.queue.name", "${queueName}", hiveNamespace, configElement);
			createPropertyElements("oozie.hive.defaults", "${nameNode}${hive_site_xml}", hiveNamespace, configElement);
			createPropertyElements("HADOOP_USER_NAME", "${wf:user()}", hiveNamespace, configElement);
			hiveElement.addContent(configElement);
			createElement("script", "${hiveScript}", hiveNamespace, hiveElement);
			parent.addContent(hiveElement);
			rootNode.addContent(4, parent);
			doc.setContent(rootNode);

			Element okElement = new Element("ok");
			okElement.setNamespace(oozieNameSpace);
			okElement.setAttribute("to", "end");
			parent.addContent(okElement);

			Element errorElement = new Element("error");
			errorElement.setNamespace(oozieNameSpace);
			errorElement.setAttribute("to", "fail");
			parent.addContent(errorElement);

		} catch (IOException io) {
			io.printStackTrace();
		} catch (JDOMException e) {
			e.printStackTrace();
		}
		return doc;
	}

	public Document getOozieWorkFlowTemplateForSQLBulkIngestion(String actionName, String scriptName,
			String workflowname, String type) {

		Element rootNode = null;
		Document doc = null;
		Namespace oozieNameSpace = Namespace.getNamespace("uri:oozie:workflow:0.4");
		Namespace hiveNamespace = Namespace.getNamespace("uri:oozie:hive-action:0.2");
		Namespace oozieShellNameSpace = Namespace.getNamespace("uri:oozie:shell-action:0.1");

		try {
			SAXBuilder builder = new SAXBuilder();
			InputStream is = this.getClass().getClassLoader().getResourceAsStream("oozie-workflow-template.xml");
			doc = (Document) builder.build(is);
			rootNode = doc.getRootElement();

			rootNode.getAttribute("name").setValue(workflowname);
			rootNode.getChild("start", Namespace.getNamespace("uri:oozie:workflow:0.4")).getAttribute("to")
					.setValue(actionName);
			List<Element> childList = rootNode.getChildren();

			for (Element element : childList) {
				if (element.getName().equalsIgnoreCase("action")) {
					element.getAttribute("name").setValue(actionName);

					Element shellElement = new Element("shell");
					shellElement.setNamespace(oozieShellNameSpace);

					createElement("job-tracker", "${jobTracker}", oozieShellNameSpace, shellElement);
					createElement("name-node", "${nameNode}", oozieShellNameSpace, shellElement);
					Element configElement = new Element("configuration", oozieShellNameSpace);
					createPropertyElements("mapred.job.queue.name", "${queueName}", oozieShellNameSpace, configElement);
					shellElement.addContent(configElement);
					element.setContent(shellElement);
					createElement("exec", "${scriptName}", oozieShellNameSpace, shellElement);
					createElement("env-var", "HADOOP_USER_NAME=${wf:user()}", oozieShellNameSpace, shellElement);
					createElement("file", "${scriptPath}", oozieShellNameSpace, shellElement);
					if (type.equalsIgnoreCase(FILE_TYPE)) {
						createElement("file", "${fileschedulerJar}", oozieShellNameSpace, shellElement);
						createElement("archive", "${fileschedulerJar}", oozieShellNameSpace, shellElement);
						createElement("archive", "${scriptPath}", oozieShellNameSpace, shellElement);
					}

					Element okElement = new Element("ok");
					okElement.setNamespace(oozieNameSpace);
					okElement.setAttribute("to", "hive-action");
					element.addContent(okElement);

					Element errorElement = new Element("error");
					errorElement.setNamespace(oozieNameSpace);
					errorElement.setAttribute("to", "fail");
					element.addContent(errorElement);

				}

			}
			Element parent = new Element("action");
			parent.setNamespace(oozieNameSpace);
			parent.setAttribute("name", "hive-action");
			Element hiveElement = new Element("hive").setNamespace(hiveNamespace);
			createElement("job-tracker", "${jobTracker}", hiveNamespace, hiveElement);
			createElement("name-node", "${nameNode}", hiveNamespace, hiveElement);
			createElement("job-xml", "hive-site.xml", hiveNamespace, hiveElement);
			Element configElement = new Element("configuration", hiveNamespace);
			createPropertyElements("mapred.job.queue.name", "${queueName}", hiveNamespace, configElement);
			createPropertyElements("oozie.hive.defaults", "${nameNode}${hive_site_xml}", hiveNamespace, configElement);
			createPropertyElements("HADOOP_USER_NAME", "${wf:user()}", hiveNamespace, configElement);
			hiveElement.addContent(configElement);
			createElement("script", "${hiveScript}", hiveNamespace, hiveElement);
			parent.addContent(hiveElement);
			rootNode.addContent(4, parent);
			doc.setContent(rootNode);

			Element okElement = new Element("ok");
			okElement.setNamespace(oozieNameSpace);
			okElement.setAttribute("to", "end");
			parent.addContent(okElement);

			Element errorElement = new Element("error");
			errorElement.setNamespace(oozieNameSpace);
			errorElement.setAttribute("to", "fail");
			parent.addContent(errorElement);

		} catch (IOException io) {
			io.printStackTrace();
		} catch (JDOMException e) {
			e.printStackTrace();
		}
		return doc;
	}

	private void createPropertyElements(String name, String value, Namespace namespace, Element root) {
		Element propElement = new Element("property", namespace);

		Element inputElement = new Element("name", namespace);
		inputElement.setText(name);

		Element inputValueElement = new Element("value", namespace);
		inputValueElement.setText(value);

		propElement.addContent(inputElement);
		propElement.addContent(inputValueElement);

		root.addContent(propElement);
	}

	private void createElement(String name, String value, Namespace namespace, Element root) {

		Element inputElement = new Element(name, namespace);
		inputElement.setText(value);

		root.addContent(inputElement);
	}

	public Document getcoordinatorTemplate(String appPath, String frequency, String workflowname) {

		Element rootNode = null;
		Document doc = null;
		try {
			SAXBuilder builder = new SAXBuilder();
			InputStream is = this.getClass().getClassLoader().getResourceAsStream("coordinator-template.xml");
			doc = (Document) builder.build(is);
			rootNode = doc.getRootElement();
			rootNode.getAttribute("name").setValue(workflowname);
			rootNode.getAttribute("frequency").setValue(frequency);

			List<Element> childList = rootNode.getChildren();
			for (Element element : childList) {
				if (element.getName().equalsIgnoreCase("action")) {

					List<Element> coordinatorChildElement = element.getChildren();
					for (Element coordinatorElement : coordinatorChildElement) {
						if (coordinatorElement.getName().equalsIgnoreCase("workflow")) {
							List<Element> configElement = coordinatorElement.getChildren();
							for (Element configElementValues : configElement) {
								if (configElementValues.getName().equalsIgnoreCase("app-path")) {
									configElementValues.setText("${nameNode}" + appPath);
								}
							}
						}
					}
				}
			}

		} catch (IOException io) {
			io.printStackTrace();
		} catch (JDOMException e) {
			e.printStackTrace();
		}
		return doc;

	}

	public Document getWorkFlowTemplate(String workflowName, String startAction) {
		Element rootNode = null;
		Document doc = null;
		try {
			SAXBuilder builder = new SAXBuilder();
			InputStream is = this.getClass().getClassLoader().getResourceAsStream("workflow-template.xml");
			doc = (Document) builder.build(is);
			rootNode = doc.getRootElement();
			rootNode.getAttribute("name").setValue(workflowName);
			rootNode.getChild("start", Namespace.getNamespace("uri:oozie:workflow:0.2")).getAttribute("to")
					.setValue(startAction);
		} catch (IOException io) {
			io.printStackTrace();
		} catch (JDOMException e) {
			e.printStackTrace();
		}
		return doc;
	}

	/*
	 * public Element getHiveActionTemplate(String stageName, String nextStage,
	 * String hqlPath) {
	 * 
	 * try { SAXBuilder hiveBuilder = new SAXBuilder(); InputStream is =
	 * this.getClass().getClassLoader().getResourceAsStream(
	 * "hiveAction-template.xml"); Document hiveDoc = (Document)
	 * hiveBuilder.build(is); Element hiveAction = hiveDoc.getRootElement();
	 * List<Element> childList = hiveAction.getChildren();
	 * hiveAction.getAttribute("name").setValue(stageName);
	 * hiveAction.setNamespace(Namespace.getNamespace("uri:oozie:workflow:0.2"))
	 * ;
	 * 
	 * // for (Element node : childList) {
	 * 
	 * 
	 * if (node.getName()!=null && node.getName().equals("action")){
	 * node.getAttribute("name").setValue(stageName);
	 * 
	 * // List<Element> actionChildElement = node.getChildren(); for (Element
	 * element : childList) { if (element.getName().equalsIgnoreCase("hive")) {
	 * List<Element> hiveChildElement = element.getChildren(); for (Element
	 * hiveElement : hiveChildElement) { if
	 * (hiveElement.getName().equalsIgnoreCase("job-xml")) {
	 * hiveElement.setText("${nameNode}${hive_site_xml}"); } else if
	 * (hiveElement.getName().equalsIgnoreCase("job-tracker")) {
	 * hiveElement.setText("${jobTracker}"); } else if
	 * (hiveElement.getName().equalsIgnoreCase("name-node")) {
	 * hiveElement.setText("${nameNode}");// } else if
	 * (hiveElement.getName().equalsIgnoreCase("script")) {
	 * hiveElement.setText("${nameNode}" + hqlPath); } else if
	 * (hiveElement.getName().equalsIgnoreCase("configuration")) { List<Element>
	 * configElement = hiveElement.getChildren(); for (Element
	 * configElementValues : configElement) { int configElementSize =
	 * configElement.size(); int maxSize = configElementSize; if
	 * (configElementSize == maxSize) { if
	 * (configElementValues.getName().equalsIgnoreCase("name")) {
	 * configElementValues.setText("mapred.job.queue.name"); } else if
	 * (configElementValues.getName().equalsIgnoreCase("value")) {
	 * configElementValues.setText("default"); } maxSize--; } else { if
	 * (configElementValues.getName().equalsIgnoreCase("name")) {
	 * configElementValues.setText("oozie.hive.defaults"); } else if
	 * (configElementValues.getName().equalsIgnoreCase("value")) {
	 * configElementValues.setText("${nameNode}${hive_site_xml}"); } } } }
	 * 
	 * } } else if (element.getName().equalsIgnoreCase("ok")) {
	 * element.getAttribute("to").setValue(nextStage); } else if
	 * (element.getName().equalsIgnoreCase("error")) {
	 * element.getAttribute("to").setValue("fail"); } }
	 * System.out.println(hiveAction.detach().getText()); return
	 * hiveAction.detach();
	 * 
	 * } catch (IOException io) { io.printStackTrace(); } catch (JDOMException
	 * e) { e.printStackTrace(); }
	 * 
	 * return null; }
	 */
	
	//hive action template for lineage
	public Element getHiveActionTemplate(String stageName, String nextStage, String hqlPath) {

		try {
			SAXBuilder shellBuilder = new SAXBuilder();
			InputStream is = this.getClass().getClassLoader().getResourceAsStream("hiveAction-template.xml");
			Document shellDoc = (Document) shellBuilder.build(is);
			Element shellAction = shellDoc.getRootElement();
			List<Element> childList = shellAction.getChildren();
			shellAction.getAttribute("name").setValue(stageName);
			shellAction.setNamespace(Namespace.getNamespace("uri:oozie:workflow:0.2"));

			for (Element element : childList) {
				if (element.getName().equalsIgnoreCase("shell")) {
					List<Element> shellChildElement = element.getChildren();
					for (Element shellElement : shellChildElement) {
						if (shellElement.getName().equalsIgnoreCase("job-tracker")) {
							shellElement.setText("${jobTracker}");
						} else if (shellElement.getName().equalsIgnoreCase("name-node")) {
							shellElement.setText("${nameNode}");
						} else if (shellElement.getName().equalsIgnoreCase("exec")) {
							shellElement.setText("${nameNode}" + hqlPath);
						} else if (shellElement.getName().equalsIgnoreCase("file")) {
							if (shellElement.getText().contains("createHiveTable"))
								shellElement.setText("${nameNode}" + hqlPath);
						} else if (shellElement.getName().equalsIgnoreCase("configuration")) {
							List<Element> configElement = shellElement.getChildren();
							for (Element configElementValues : configElement) {
								int configElementSize = configElement.size();
								int maxSize = configElementSize;
								if (configElementSize == maxSize) {
									if (configElementValues.getName().equalsIgnoreCase("name")) {
										configElementValues.setText("mapred.job.queue.name");
									} else if (configElementValues.getName().equalsIgnoreCase("value")) {
										configElementValues.setText("default");
									}
									maxSize--;
								}
							}
						}

					}
				} else if (element.getName().equalsIgnoreCase("ok")) {
					element.getAttribute("to").setValue(nextStage);
				} else if (element.getName().equalsIgnoreCase("error")) {
					element.getAttribute("to").setValue("fail");
				}

			}
			return shellAction.detach();
		} catch (IOException io) {
			io.printStackTrace();
		} catch (JDOMException e) {
			e.printStackTrace();
		}
		return null;
	}

	public Element getPigActionTemplate(String stageName, String nextStage, String pigScriptPath, String stageInputPath,
			String stageOutputPath) {

		try {
			SAXBuilder pigBuilder = new SAXBuilder();
			InputStream is = this.getClass().getClassLoader().getResourceAsStream("pigAction-template.xml");
			Document pigDoc = (Document) pigBuilder.build(is);
			Element pigAction = pigDoc.getRootElement();
			List<Element> childList = pigAction.getChildren();
			pigAction.getAttribute("name").setValue(stageName);
			pigAction.setNamespace(Namespace.getNamespace("uri:oozie:workflow:0.2"));

			for (Element element : childList) {
				if (element.getName().equalsIgnoreCase("pig")) {
					List<Element> pigChildElement = element.getChildren();
					for (Element pigElement : pigChildElement) {
						if (pigElement.getName().equalsIgnoreCase("job-tracker")) {
							pigElement.setText("${jobTracker}");
						} else if (pigElement.getName().equalsIgnoreCase("name-node")) {
							pigElement.setText("${nameNode}");//
						} else if (pigElement.getName().equalsIgnoreCase("script")) {
							pigElement.setText("${nameNode}" + pigScriptPath);
						} else if (pigElement.getName().equalsIgnoreCase("configuration")) {
							List<Element> configElement = pigElement.getChildren();
							for (Element configElementValues : configElement) {
								int configElementSize = configElement.size();
								int maxSize = configElementSize;
								if (configElementSize == maxSize) {
									if (configElementValues.getName().equalsIgnoreCase("name")) {
										configElementValues.setText("mapred.job.queue.name");
									} else if (configElementValues.getName().equalsIgnoreCase("value")) {
										configElementValues.setText("default");
									}
									maxSize--;
								}
							}
						} else if (pigElement.getName().equalsIgnoreCase("param")) {
							/*
							 * if(pigElement.getText().contains("INPUT")){
							 * pigElement.setText("INPUT=${nameNode}"+
							 * stageInputPath); }else {
							 * pigElement.setText("OUTPUT=${nameNode}"+
							 * stageOutputPath); }
							 */}

					}
				} else if (element.getName().equalsIgnoreCase("ok")) {
					element.getAttribute("to").setValue(nextStage);
				} else if (element.getName().equalsIgnoreCase("error")) {
					element.getAttribute("to").setValue("fail");
				}

			}

			return pigAction.detach();
		} catch (IOException io) {
			io.printStackTrace();
		} catch (JDOMException e) {
			e.printStackTrace();
		}

		return null;
	}

	public Element getShellActionTemplate(String stageName, String nextStage, String shellScriptPath) {

		try {
			SAXBuilder shellBuilder = new SAXBuilder();
			InputStream is = this.getClass().getClassLoader().getResourceAsStream("shellActionTemplate.xml");
			Document shellDoc = (Document) shellBuilder.build(is);
			Element shellAction = shellDoc.getRootElement();
			List<Element> childList = shellAction.getChildren();
			shellAction.getAttribute("name").setValue(stageName);
			shellAction.setNamespace(Namespace.getNamespace("uri:oozie:workflow:0.2"));

			for (Element element : childList) {
				if (element.getName().equalsIgnoreCase("shell")) {
					List<Element> shellChildElement = element.getChildren();
					for (Element shellElement : shellChildElement) {
						if (shellElement.getName().equalsIgnoreCase("job-tracker")) {
							shellElement.setText("${jobTracker}");
						} else if (shellElement.getName().equalsIgnoreCase("name-node")) {
							shellElement.setText("${nameNode}");//
						} else if (shellElement.getName().equalsIgnoreCase("exec")) {
							shellElement.setText("${nameNode}" + shellScriptPath);
						} else if (shellElement.getName().equalsIgnoreCase("file")) {
							shellElement.setText("${nameNode}" + shellScriptPath);
						} else if (shellElement.getName().equalsIgnoreCase("configuration")) {
							List<Element> configElement = shellElement.getChildren();
							for (Element configElementValues : configElement) {
								int configElementSize = configElement.size();
								int maxSize = configElementSize;
								if (configElementSize == maxSize) {
									if (configElementValues.getName().equalsIgnoreCase("name")) {
										configElementValues.setText("mapred.job.queue.name");
									} else if (configElementValues.getName().equalsIgnoreCase("value")) {
										configElementValues.setText("default");
									}
									maxSize--;
								}
							}
						}

					}
				} else if (element.getName().equalsIgnoreCase("ok")) {
					element.getAttribute("to").setValue(nextStage);
				} else if (element.getName().equalsIgnoreCase("error")) {
					element.getAttribute("to").setValue("fail");
				}

			}
			return shellAction.detach();
		} catch (IOException io) {
			io.printStackTrace();
		} catch (JDOMException e) {
			e.printStackTrace();
		}
		return null;
	}

	public Element getTransformActionTemplate(String stageName, String nextStage, String shellScriptPath, String type,
			String schema, String inputInfo, String jarPath) {

		try {
			SAXBuilder shellBuilder = new SAXBuilder();
			InputStream is = this.getClass().getClassLoader().getResourceAsStream("shellActionTemplate.xml");
			Document shellDoc = (Document) shellBuilder.build(is);
			Element shellAction = shellDoc.getRootElement();
			List<Element> childList = shellAction.getChildren();
			shellAction.getAttribute("name").setValue(stageName);
			shellAction.setNamespace(Namespace.getNamespace("uri:oozie:workflow:0.2"));

			for (Element element : childList) {
				if (element.getName().equalsIgnoreCase("shell")) {
					List<Element> shellChildElement = element.getChildren();
					for (Element shellElement : shellChildElement) {
						if (shellElement.getName().equalsIgnoreCase("job-tracker")) {
							shellElement.setText("${jobTracker}");
						} else if (shellElement.getName().equalsIgnoreCase("name-node")) {
							shellElement.setText("${nameNode}");//
						} else if (shellElement.getName().equalsIgnoreCase("exec")) {
							shellElement.setText("${nameNode}" + shellScriptPath);
						} else if (shellElement.getName().equalsIgnoreCase("file")) {
							if (shellElement.getText().contains("createHiveTable"))
								shellElement.setText("${nameNode}" + shellScriptPath);
							else if (shellElement.getText().contains("Transformation")) {
								shellElement.setText("${nameNode}" + jarPath);
							}
						} else if (shellElement.getName().equalsIgnoreCase("argument")) {
							if (shellElement.getText().equalsIgnoreCase("arg1"))
								shellElement.setText(type.replace(" ", "_"));
							else if (shellElement.getText().equalsIgnoreCase("arg2")) {
								System.out.println("schema=====" + schema);
								shellElement.setText(schema);
							} else {
								if (inputInfo.startsWith(";") && type.equalsIgnoreCase("Join")) {
									String start = inputInfo.substring(1, inputInfo.indexOf(';', 1));
									inputInfo = start + inputInfo;
									shellElement.setText(inputInfo);
								} else {
									shellElement.setText(inputInfo);
								}
							}
						} else if (shellElement.getName().equalsIgnoreCase("configuration")) {
							List<Element> configElement = shellElement.getChildren();
							for (Element configElementValues : configElement) {
								int configElementSize = configElement.size();
								int maxSize = configElementSize;
								if (configElementSize == maxSize) {
									if (configElementValues.getName().equalsIgnoreCase("name")) {
										configElementValues.setText("mapred.job.queue.name");
									} else if (configElementValues.getName().equalsIgnoreCase("value")) {
										configElementValues.setText("default");
									}
									maxSize--;
								}
							}
						}

					}
				} else if (element.getName().equalsIgnoreCase("ok")) {
					element.getAttribute("to").setValue(nextStage);
				} else if (element.getName().equalsIgnoreCase("error")) {
					element.getAttribute("to").setValue("fail");
				}

			}
			return shellAction.detach();
		} catch (IOException io) {
			io.printStackTrace();
		} catch (JDOMException e) {
			e.printStackTrace();
		}
		return null;
	}

	public Element getMRActionTemplate(String stageName, String nextStage, String mapperClassName,
			String reducerClassName, String stageInputPath, String stageOutputPath, String outKeyClassName,
			String outValueClassName) {

		try {
			SAXBuilder mapRedBuilder = new SAXBuilder();
			InputStream is = this.getClass().getClassLoader().getResourceAsStream("mapRedAction-template.xml");
			Document mapRedDoc = (Document) mapRedBuilder.build(is);
			Element mapRedAction = mapRedDoc.getRootElement();
			List<Element> childList = mapRedAction.getChildren();
			mapRedAction.getAttribute("name").setValue(stageName);
			mapRedAction.setNamespace(Namespace.getNamespace("uri:oozie:workflow:0.2"));

			for (Element element : childList) {
				if (element.getName().equalsIgnoreCase("map-reduce")) {

					List<Element> mapRedChildElement = element.getChildren();
					for (Element mapRedElement : mapRedChildElement) {
						if (mapRedElement.getName().equalsIgnoreCase("job-tracker")) {
							mapRedElement.setText("${jobTracker}");
						} else if (mapRedElement.getName().equalsIgnoreCase("name-node")) {
							mapRedElement.setText("${nameNode}");//
						} else if (mapRedElement.getName().equalsIgnoreCase("configuration")) {
							List<Element> configElement = mapRedElement.getChildren();
							int count = 0;
							String value = "";
							int numConfig = configElement.size();
							for (Element configElementValues : configElement) {
								if (count <= numConfig) {
									switch (count) {
									case 0:
										value = "true";
										break;
									case 1:
										value = "true";
										break;
									case 2:
										value = "default";
										break;
									case 3:
										value = mapperClassName;
										break;
									case 4:
										value = reducerClassName;
										break;
									/*
									 * case 5: value=""; break;
									 */
									case 5:
										value = "${nameNode}" + stageInputPath;
										break;
									case 6:
										value = "${nameNode}" + stageOutputPath;
										break;
									case 7:
										if (outKeyClassName.trim().isEmpty()) {
											outKeyClassName = "org.apache.hadoop.io.Text";
										}
										value = outKeyClassName;
										break;
									case 8:
										if (outValueClassName.trim().isEmpty()) {
											outValueClassName = "org.apache.hadoop.io.IntWritable";
										}
										value = outValueClassName;
										break;
									}
									if (configElementValues
											.getChild("value", Namespace.getNamespace("uri:oozie:workflow:0.2"))
											.getName().equalsIgnoreCase("value")) {
										configElementValues
												.getChild("value", Namespace.getNamespace("uri:oozie:workflow:0.2"))
												.setText(value);
									}
									count++;
								}
							}
						} else if (mapRedElement.getName().equalsIgnoreCase("prepare")) {
							mapRedElement.getChild("delete", Namespace.getNamespace("uri:oozie:workflow:0.2"))
									.getAttribute("path").setValue("${nameNode}" + stageOutputPath);
						}
					}
				} else if (element.getName().equalsIgnoreCase("ok")) {
					element.getAttribute("to").setValue(nextStage);
				} else if (element.getName().equalsIgnoreCase("error")) {
					element.getAttribute("to").setValue("fail");
				}

			}

			return mapRedAction.detach();
		} catch (IOException io) {
			io.printStackTrace();
		} catch (JDOMException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * Method to write contents to file on Disk.
	 * 
	 * @param content
	 *            Content to be written to File
	 * @param filePath
	 *            absolute path of output file.
	 */
	public void writeToFile(String content, String filePath) {

		try {
			File file = new File(filePath);

			// Create parent directory if not exists.
			File directory = new File(file.getParentFile().getAbsolutePath());
			directory.mkdirs();

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(content);
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void saveWorkFlowXML(Document doc, String wfLocation) {
		try {
			XMLOutputter xmlOutput = new XMLOutputter();
			xmlOutput.setFormat(Format.getPrettyFormat());
			xmlOutput.output(doc, new FileWriter(wfLocation));
		} catch (IOException e) {
			e.printStackTrace();
		}

		// xmlOutput.output(doc, System.out);

	}

	public void endWorkflowXML(Element rootElement) {

		Element killElement = new Element("kill");
		killElement.setAttribute(new Attribute("name", "fail"));
		killElement.setNamespace(Namespace.getNamespace("uri:oozie:workflow:0.2"));

		Element messageNode = new Element("message");
		messageNode.setText("Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]");
		messageNode.setNamespace(Namespace.getNamespace("uri:oozie:workflow:0.2"));
		killElement.addContent(messageNode);

		rootElement.addContent(killElement);

		Element endElement = new Element("end");
		endElement.setAttribute(new Attribute("name", "end"));
		endElement.setNamespace(Namespace.getNamespace("uri:oozie:workflow:0.2"));
		rootElement.addContent(endElement);

	}

	public void buildForkNode(Element rootElement, Stage stage) {
		Element fork = new Element("fork");
		fork.setAttribute(new Attribute("name", stage.nextAction));
		fork.setNamespace(Namespace.getNamespace("uri:oozie:workflow:0.2"));

		for (String action : stage.child) {
			Element path = new Element("path");
			path.setAttribute(new Attribute("start", action));
			path.setNamespace(Namespace.getNamespace("uri:oozie:workflow:0.2"));
			fork.addContent(path);
		}
		rootElement.addContent(fork);
	}

	public void buildJoinNode(Element rootElement, Stage stage) {
		List<Element> join = rootElement.getChildren("join", Namespace.getNamespace("uri:oozie:workflow:0.2"));
		for (Element element : join) {
			if ((element.getAttribute("name").getValue()).equalsIgnoreCase(stage.nextAction))
				return;
		}
		Element fork = new Element("join");
		fork.setAttribute(new Attribute("name", stage.nextAction));
		fork.setAttribute(new Attribute("to", stage.child[0]));
		fork.setNamespace(Namespace.getNamespace("uri:oozie:workflow:0.2"));
		rootElement.addContent(fork);
	}

}
