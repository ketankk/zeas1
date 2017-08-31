package transformationRule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

public class ZEASTransformationRule {
	public static Logger LOG = Logger.getLogger(ZEASTransformationRule.class);

	static String msg = "17/05/29 14:52:47 TRACE ServletInvocableHandlerMethod: Method [com.itc.zeas.profile.ProfileController.getprofileRunStatus] returned [<200 OK,{13391=Completed, 13421=New, 13387=Completed, 13656=Completed, 13691=New, 13383=New, 13652=Completed, 13687=Completed, 13415=Completed, 13759=New, 13379=Completed, 13683=Completed, 13411=Completed, 13696=Started, 13403=Completed, 13375=New},{}>]";
	private final static String BODY = "Method [com.itc.zeas.profile.ProfileController.getprofileRunStatus] returned";

	public static final String consumerMessage = null;

	public String getRules(String msg) {

		return msg;

	}

	public static Map<String, Map<Integer, String>> readConsumerMessage(List<String> msgs) {
		String timeStamp = null;
		//System.out.println("msg &&&&&&&" + msgs.toString());
		for (String msg : msgs) {

			String jobdetails = null;

			Map<String, Map<Integer, String>> timstampMap = new HashMap<String, Map<Integer, String>>();

			if (msg != null) {
				System.out.println("Message is "+msg);
				if (msg.contains(ZEASTransformationRule.BODY)) {

					// System.out.println("Message contains body"+msg);
					timeStamp = msg.substring(0, 17);

					int index = msg.indexOf(BODY) + BODY.length() + 2;

					jobdetails = msg.substring(index + 9, msg.length() - 5);
					System.out.println("jobDet " + jobdetails);
				}
				if (jobdetails != null) {
					String jobData[] = jobdetails.split(", ");

					Map<Integer, String> jobMap = new HashMap<Integer, String>();
					for (int i = 0; i < jobData.length; i++) {

						String[] str = jobData[i].split("=");
						Integer id = Integer.parseInt(str[0]);

						String process = str[1];

						jobMap.put(id, process);

					}
					timstampMap.put(timeStamp, jobMap);
					return timstampMap;

				}
			}
			// System.out.println("$$$$$$...."+timstampMap.toString());
		}
		return new HashMap<>();
	}

	static public String transformedRDD(List<String> rdd) {
		Map<String, Map<Integer, String>> transformedData = readConsumerMessage(rdd);

		StringBuilder builder = new StringBuilder();

		for (Entry<String, Map<Integer, String>> entry : transformedData.entrySet()) {
			String timeStamp = entry.getKey();

			Map<Integer, String> value = entry.getValue();

			for (Entry<Integer, String> entry1 : value.entrySet()) {

				Integer jobID = entry1.getKey();
				String jobStatus = entry1.getValue();

				builder.append(timeStamp + "," + jobID + "," + jobStatus + "\n");
				// System.out.println(builder.toString());

			}
			// separate each entry by line break
			// builder.append("\n");

		}
		//builder.replace(builder.length()-1, builder.length()-1, "");
//		builder.deleteCharAt(builder.length()-1);

		System.out.println("Returning string after transfromation " + builder);
		if (builder.length() > 0)
			return builder.toString().replace("}", "").trim();
		return null;
	}

	
}
