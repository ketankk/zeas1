package com.itc.zeas.validation.mask;

public interface MaskingConstants {
	
	String PROTECTION_TYPE_REMOVAL="remove";
	String PROTECTION_TYPE_OBFUSCATE="obfuscate";
	String PII_CATEGORY_NUMERIC="numeric";
	String PII_CATEGORY_EMAIL="email";
	String PII_CATEGORY_GENDER="gender";
	String GENDER_MALE="male";
	String GENDER_FEMALE="female";
	Character MASKING_CHARACTER=new Character('*');
	
}
