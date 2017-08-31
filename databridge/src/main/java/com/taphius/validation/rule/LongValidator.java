package com.taphius.validation.rule;

public class LongValidator implements ValidationRule {
	
	private boolean enabled;
	private PIIRule pii;

	@Override
	public boolean validate(Object obj) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getError() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getRuleName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getExpected() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getColumn() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean typeCheck(Object obj) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PIIRule getPIIRule() {
		// TODO Auto-generated method stub
		return this.pii;
	}

	@Override
	public void setPIIRule(PIIRule pii) {
		this.pii =pii;

	}

	@Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void enableRule(String isEnabled) {
        if("YES".equalsIgnoreCase(isEnabled)){
            enabled = true;
        }else {
            enabled = false;
        }
    }  

}
