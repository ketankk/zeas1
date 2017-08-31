package com.itc.zeas.validation.mask;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public class PiiProtectorUtility {
	
	private static final String EMAIL_PATTERN = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
			+ "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
	private static final Logger logger = Logger.getLogger(PiiProtectorUtility.class);

	/**
	 * Perform security protection operation on input PII data depending upon
	 * input Protection type
	 * 
	 * @param inputPiiData
	 *            input on which security protection will be performed
	 * @param protectionType
	 *            specify kind of security protection to be performed.
	 * @return secured cleansed data
	 */
	public static String performProtection(String inputPiiData,
			String protectionType) {
		
		logger.debug("inside function performProtection");
		String cleansedPiiData = null;
		if(inputPiiData.isEmpty() || inputPiiData==null){
			logger.info("Invalid PII Data "+ protectionType+" it should not be empty or Null");
			cleansedPiiData=inputPiiData;
			return cleansedPiiData;
		}
		switch (protectionType) {
		case MaskingConstants.PROTECTION_TYPE_OBFUSCATE:
			logger.debug("PII Data Protection Type is "+ protectionType);
			cleansedPiiData = performObfuscate(inputPiiData);
			break;
		case MaskingConstants.PROTECTION_TYPE_REMOVAL:
			// for this case empty string need to be passed to the caller
			logger.debug("PII Data Protection Type is "+ protectionType);
			cleansedPiiData = "";
			break;
		default:
			// log error message
			// " invalid protection type this should be either Constants.PROTECTION_TYPE_OBFUSCATE or Constants.PROTECTION_TYPE_REMOVAL".
//			logger.error("Invalid PII Data Protection Type "+ protectionType);
//			throw new RuntimeException(
//					"invalid protection type this should be either "
//							+ MaskingConstants.PROTECTION_TYPE_OBFUSCATE + "or "
//							+ MaskingConstants.PROTECTION_TYPE_REMOVAL);
			logger.debug("PII Data Protection Type is "+ protectionType);
			cleansedPiiData = performObfuscate(inputPiiData);
			break;
		}
		logger.debug("Cleansed PII data is "+ cleansedPiiData);
		return cleansedPiiData;
	}

	/**
	 * Gives obfuscated output for given input
	 * 
	 * @param inputPiiData
	 *            input parameter that needs to be obfuscated
	 * @return a string representing obfuscated output
	 */
	private static String performObfuscate(String inputPiiData) {
		String piiCategory = getPiiCategory(inputPiiData);
		String maskedPiiData = getMaskedData(inputPiiData, piiCategory);
		return maskedPiiData;
	}

	/**
	 * Gives masked data for given input
	 * 
	 * @param inputPiiData
	 *            input parameter that needs to be masked
	 * @param piiCategory
	 *            PII category of input that to be masked
	 * @return a string representing masked output data
	 */
	private static String getMaskedData(String inputPiiData, String piiCategory) {
		MaskingOutDataStrategy maskingOutDataStrategy = new MaskingOutDataStrategy();
		return maskingOutDataStrategy.mask(inputPiiData, piiCategory);
	}

	/**
	 * Gives PII category
	 * 
	 * @param inputPiiInfo
	 *            input parameter for which PII category to be found
	 * @return string value representing PII category
	 */
	private static String getPiiCategory(String inputPiiInfo) {
		String piiCategory = MaskingConstants.PII_CATEGORY_NUMERIC;
		if (inputPiiInfo.equalsIgnoreCase(MaskingConstants.GENDER_MALE)
				|| inputPiiInfo.equalsIgnoreCase(MaskingConstants.GENDER_FEMALE)) {
			piiCategory = MaskingConstants.PII_CATEGORY_GENDER;
		} else if (isEmail(inputPiiInfo)) {
			piiCategory = MaskingConstants.PII_CATEGORY_EMAIL;
		}
		return piiCategory;
	}

	/**
	 * Checks whether given input parameter is Email or not
	 * 
	 * @param inputPiiInfo
	 *            input parameter which will be checked
	 * 
	 * @return true if input parameter is Email otherwise false
	 */
	private static boolean isEmail(String inputPiiInfo) {
		Pattern pattern = Pattern.compile(EMAIL_PATTERN);
		Matcher matcher = pattern.matcher(inputPiiInfo);
		return matcher.matches();
	}

}
