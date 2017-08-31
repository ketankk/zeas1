package com.itc.zeas.validation.mask;


public class MaskingOutDataStrategy implements MaskingStrategy{

	@Override
	public String mask(String dataToBeMasked, String dataCategory) {
		StringBuilder maskedPiiData = null;
		switch (dataCategory) {
		case MaskingConstants.PII_CATEGORY_NUMERIC:
			int inputPiiDataLength = dataToBeMasked.length();
			StringBuilder inputPiiToBeMasked = new StringBuilder(dataToBeMasked);
			if (inputPiiDataLength <= 4) {
				for (int characterIndex = 0; characterIndex < inputPiiDataLength; characterIndex++) {
					inputPiiToBeMasked.setCharAt(characterIndex,
							MaskingConstants.MASKING_CHARACTER);
				}
			} else if (inputPiiDataLength <= 6) {
				for (int characterIndex = 1; characterIndex < (inputPiiDataLength - 1); characterIndex++) {
					inputPiiToBeMasked.setCharAt(characterIndex,
							MaskingConstants.MASKING_CHARACTER);
				}
			} else {
				for (int characterIndex = 2; characterIndex < (inputPiiDataLength - 2); characterIndex++) {
					inputPiiToBeMasked.setCharAt(characterIndex,
							MaskingConstants.MASKING_CHARACTER);
				}
			}
			maskedPiiData = inputPiiToBeMasked;
			break;
		case MaskingConstants.PII_CATEGORY_EMAIL:
			maskedPiiData = new StringBuilder(dataToBeMasked);
			int posOfLastOccurancOfAtTheRate = dataToBeMasked.lastIndexOf('@');
			if(posOfLastOccurancOfAtTheRate ==0){
				//do nothing
			}
			else if (posOfLastOccurancOfAtTheRate <= 4) {
				for (int count = 0; count < posOfLastOccurancOfAtTheRate; count++) {
					maskedPiiData.setCharAt(count, MaskingConstants.MASKING_CHARACTER);
				}
			}else if(posOfLastOccurancOfAtTheRate <= 7){
				for (int count = 1; count < (posOfLastOccurancOfAtTheRate - 1); count++) {
					maskedPiiData.setCharAt(count, MaskingConstants.MASKING_CHARACTER);
				}
			} else {
				for (int count = 2; count < (posOfLastOccurancOfAtTheRate - 2); count++) {
					maskedPiiData.setCharAt(count, MaskingConstants.MASKING_CHARACTER);
				}
			}
			break;
		case MaskingConstants.PII_CATEGORY_GENDER:
			maskedPiiData = new StringBuilder(
					MaskingConstants.MASKING_CHARACTER.toString());
			break;
		}
		return maskedPiiData.toString();
	}

}
