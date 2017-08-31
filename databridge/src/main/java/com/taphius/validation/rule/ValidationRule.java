package com.taphius.validation.rule;
/**
 * Defines interface for performing validation.
 * Implementing classes has to provide logic for custom validation.
 * @author 16795
 *
 */
public interface ValidationRule {
    /**
     * function to perform validation.
     * @param obj Object upon which certain validation needs to be performed.
     * @return <code>true</code> if validation passes else <code>false</code>
     */
    public boolean validate(Object obj);
    
    /**
     * Helper method returns error message in case validation fails.
     * This comes handy to log the error message in case of failure.
     * @return {@link String} validation error message
     */
    public String getError();
    
    /**
     * Name of the validation rule.
     * @return String name of the validation rule.
     */
    public String getRuleName();
    
    /**
     * Method returns the expected value for which this rule passes.
     * Basically for String it would return expected range etc.
     * @return String expected range values.
     */
    public String getExpected();
    /**
     * Method returns the column name for which this validation rule is applied.
     * @return {@link String} name of the column.
     */
    public String getColumn();
    
    public boolean typeCheck(Object obj);
    
    /**
     * Method returns the value found, ie returns the value for which validation is applied.
     * @return {@link String} validated value.
     */
    public String getValue();
    
    public PIIRule getPIIRule();
    
    public void setPIIRule(PIIRule pii);
    
    public boolean isEnabled();
    
    public void enableRule(String isEnabled);
}
