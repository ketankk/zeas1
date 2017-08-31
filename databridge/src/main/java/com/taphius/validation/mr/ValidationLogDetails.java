package com.taphius.validation.mr;

import java.io.Serializable;
import java.text.DecimalFormat;

public class ValidationLogDetails implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -3901202351182531082L;
    private long noOfQuarantine;
    private long noOfRangeFails;
    private long noOfRegexFails;
    private long noOfFixedlLengthFails;
    private long noOfWhiteListFails;
    private long noOfBlackListFails;
    private long noOfMandatoryFails;
    private long noOfDataTypeMismatchFails;
    private long noOfColumnMismatchFails;
    private long noOfotherFails;
    //to calculate the  percentage of data
    long noOfTotalRecords;



    public long getNoOfQurantile() {
        noOfQuarantine= noOfRangeFails+noOfRegexFails+noOfFixedlLengthFails+noOfWhiteListFails+noOfBlackListFails+noOfMandatoryFails+noOfDataTypeMismatchFails+noOfColumnMismatchFails;
        return noOfQuarantine;
    }


    public long getNoOfRangeFails() {
        return noOfRangeFails;
    }

    public void setNoOfRangeFails(long noOfRangeFails) {
        this.noOfRangeFails =noOfRangeFails;
    }

    public long getNoOfRegexFails() {
        return noOfRegexFails;
    }

    public void setNoOfRegexFails(long noOfRegexFails) {
        this.noOfRegexFails = noOfRegexFails;
    }

    public long getNoOfFixedlLengthFails() {
        return noOfFixedlLengthFails;
    }

    public void setNoOfFixedlLengthFails(long noOfFixedlLengthFails) {
        this.noOfFixedlLengthFails = noOfFixedlLengthFails;
    }

    public long getNoOfWhiteListFails() {
        return noOfWhiteListFails;
    }

    public void setNoOfWhiteListFails(long noOfWhiteListFails) {
        this.noOfWhiteListFails = noOfWhiteListFails;
    }

    public long getNoOfBlackListFails() {
        return noOfBlackListFails;
    }

    public void setNoOfBlackListFails(long noOfBlackListFails) {
        this.noOfBlackListFails = noOfBlackListFails;
    }

    public long getNoOfMandatoryFails() {
        return noOfMandatoryFails;
    }

    public void setNoOfMandatoryFails(long noOfMandatoryFails) {
        this.noOfMandatoryFails = noOfMandatoryFails;
    }

    public long getNoOfDataTypeMismatchFails() {
        return noOfDataTypeMismatchFails;
    }

    public void setNoOfDataTypeMismatchFails(long noOfDataTypeMismatchFails) {
        this.noOfDataTypeMismatchFails = noOfDataTypeMismatchFails;
    }


    public long getNoOfColumnMismatchFails() {
        return noOfColumnMismatchFails;
    }


    public void setNoOfColumnMismatchFails(long noOfColumnMismatchFails) {
        this.noOfColumnMismatchFails = noOfColumnMismatchFails;
    }


    public long getNoOfotherFails() {
        return noOfotherFails;
    }


    public void setNoOfotherFails(long noOfotherFails) {
        this.noOfotherFails = noOfotherFails;
    }


    public long getNoOfTotalRecords() {
        return noOfTotalRecords;
    }


    public void setNoOfTotalRecords(long noOfTotalRecords) {
        this.noOfTotalRecords = noOfTotalRecords;
    }

    @Override
    public String toString() {

        StringBuilder logs= new StringBuilder("");
        DecimalFormat df = new DecimalFormat("###.##");

        if((this.getNoOfQurantile()*100/this.getNoOfTotalRecords())>0){
            logs.append(noOfQuarantine+" quarantine records  ("+df.format(noOfQuarantine*100/noOfTotalRecords)+" %).\n");


            if((this.getNoOfRangeFails()*100/this.getNoOfTotalRecords())>0){
                logs.append(noOfRangeFails+" Range validation fails ("+df.format(noOfRangeFails*100/noOfTotalRecords)+" %).\n");
            }

            if((this.getNoOfFixedlLengthFails()*100/this.getNoOfTotalRecords())>0){
                logs.append(noOfFixedlLengthFails+" Fixed Length validation fails  ("+df.format(noOfFixedlLengthFails*100/noOfTotalRecords)+" %).\n");
            }

            if((this.getNoOfRegexFails()*100/this.getNoOfTotalRecords())>0){
                logs.append(noOfRegexFails+" Regex validation fails  ("+df.format(noOfRegexFails*100/noOfTotalRecords)+" %).\n");
            }

            if((this.getNoOfWhiteListFails()*100/this.getNoOfTotalRecords())>0){
                logs.append(noOfWhiteListFails+" White List validation fails  ("+df.format(noOfWhiteListFails*100/noOfTotalRecords)+" %).\n");
            }

            if((this.getNoOfBlackListFails()*100/this.getNoOfTotalRecords())>0){
                logs.append(noOfBlackListFails+" Balck List validation fails  ("+df.format(noOfBlackListFails*100/noOfTotalRecords)+" %).\n");
            }

            if((this.getNoOfMandatoryFails()*100/this.getNoOfTotalRecords())>0){
                logs.append(noOfMandatoryFails+" Mandatory validation fails  ("+df.format(noOfMandatoryFails*100/noOfTotalRecords)+" %).\n");
            }

            if((this.getNoOfDataTypeMismatchFails()*100/this.getNoOfTotalRecords())>0){
                logs.append(noOfDataTypeMismatchFails+" Data type mismatch validation fails  ("+df.format(noOfDataTypeMismatchFails*100/noOfTotalRecords)+" %).\n");
            }

            if((this.getNoOfColumnMismatchFails()*100/this.getNoOfTotalRecords())>0){
                logs.append(noOfColumnMismatchFails+" Column size mismatch validation fails  ("+df.format(noOfColumnMismatchFails*100/noOfTotalRecords)+" %).\n");
            }
            if((this.getNoOfotherFails()*100/this.getNoOfTotalRecords())>0){
                logs.append(noOfotherFails+" Others validation fails  ("+df.format(noOfotherFails*100/noOfTotalRecords)+" %).\n");
            }
        }

        //		String ss= noOfQuarantine+" quarantine records  ("+df.format(noOfQuarantine*100/noOfTotalRecords)+" %).\n"+
        //				   noOfRangeFails+" Range validation fails ("+df.format(noOfRangeFails*100/noOfTotalRecords)+" %).\n"+
        //				   noOfFixedlLengthFails+" Fixed Length validation fails  ("+df.format(noOfFixedlLengthFails*100/noOfTotalRecords)+" %).\n"+
        //				   noOfRegexFails+" Regex validation fails  ("+df.format(noOfRegexFails*100/noOfTotalRecords)+" %).\n"+
        //				   noOfWhiteListFails+" White List validation fails  ("+df.format(noOfWhiteListFails*100/noOfTotalRecords)+" %).\n"+
        //				   noOfBlackListFails+" Balck List validation fails  ("+df.format(noOfBlackListFails*100/noOfTotalRecords)+" %).\n"+
        //				   noOfMandatoryFails+" Mandatory validation fails  ("+df.format(noOfMandatoryFails*100/noOfTotalRecords)+" %).\n"+
        //				   noOfDataTypeMismatchFails+" Data type mismatch validation fails  ("+df.format(noOfDataTypeMismatchFails*100/noOfTotalRecords)+" %).\n"+
        //				   noOfColumnMismatchFails+" Column size mismatch validation fails  ("+df.format(noOfColumnMismatchFails*100/noOfTotalRecords)+" %).\n"+
        //				   noOfotherFails+" Others validation fails  ("+df.format(noOfotherFails*100/noOfTotalRecords)+" %).\n";

        return logs.toString();
    }

}
