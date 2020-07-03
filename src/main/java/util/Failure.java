package util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;

public class Failure {

    private String schoolYear;
    private int busbreakdownId;
    private String runType;
    private String busNo;
    private String routeNumber;
    private String reason;
    private String schoolsServiced;
    private long occurredOn;
    private DateTime createdOn;
    private String boro;
    private String busCompanyName;
    private int howLongDelayed;
    private int numberOfStudentsOnTheBus;
    private String hasContractorNotifiedSchools;
    private String getHasContractorNotifiedParents;
    private String haveYouAlertedOPT;
    private DateTime informedOn;
    private String incidentNumber;
    private String lastUpdatedOn;
    private String breakdownOrRunningLate;
    private String schoolAgeOrPrek;
    private int flag; // # volte in cui compare tipologia di reason
    private int rank; // posizione in classifica

    public Failure() {}

    public Failure(String schoolYear, int busbreakdownId, String runType, String busNo, String routeNumber,
                   String reason, String schoolsServiced, long occurredOn, DateTime createdOn,
                   String boro, String busCompanyName, int howLongDelayed, int numberOfStudentsOnTheBus,
                   String hasContractorNotifiedSchools, String getHasContractorNotifiedParents,
                   String haveYouAlertedOPT, DateTime informedOn, String incidentNumber, String lastUpdatedOn,
                   String breakdownOrRunningLate, String schoolAgeOrPrek) {

        this.schoolYear = schoolYear;
        this.busbreakdownId = busbreakdownId;
        this.runType = runType;
        this.busNo = busNo;
        this.routeNumber = routeNumber;
        this.reason = reason;
        this.schoolsServiced = schoolsServiced;
        this.occurredOn = occurredOn;
        this.createdOn = createdOn;
        this.boro = boro;
        this.busCompanyName = busCompanyName;
        this.howLongDelayed = howLongDelayed;
        this.numberOfStudentsOnTheBus = numberOfStudentsOnTheBus;
        this.hasContractorNotifiedSchools = hasContractorNotifiedSchools;
        this.getHasContractorNotifiedParents = getHasContractorNotifiedParents;
        this.haveYouAlertedOPT = haveYouAlertedOPT;
        this.informedOn = informedOn;
        this.incidentNumber = incidentNumber;
        this.lastUpdatedOn = lastUpdatedOn;
        this.breakdownOrRunningLate = breakdownOrRunningLate;
        this.schoolAgeOrPrek = schoolAgeOrPrek;
    }

    public Failure(long occurredOn, String boro, int howLongDelayed, String reason, int flag, int rank) {
        this.occurredOn = occurredOn;
        this.boro = boro;
        this.howLongDelayed = howLongDelayed;
        this.reason = reason;
        this.flag = flag;
        this.rank = rank;
    }

    public String toJson() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(this);
    }

    public String getSchoolYear() {
        return schoolYear;
    }

    public int getBusbreakdownId() {
        return busbreakdownId;
    }

    public String getRunType() {
        return runType;
    }

    public String getBusNo() {
        return busNo;
    }

    public String getRouteNumber() {
        return routeNumber;
    }

    public String getReason() {
        return reason;
    }

    public String getSchoolsServiced() {
        return schoolsServiced;
    }

    public long getOccurredOn() {
        return occurredOn;
    }

    public DateTime getCreatedOn() {
        return createdOn;
    }

    public String getBoro() {
        return boro;
    }

    public String getBusCompanyName() {
        return busCompanyName;
    }

    public int getHowLongDelayed() {
        return howLongDelayed;
    }

    public int getNumberOfStudentsOnTheBus() {
        return numberOfStudentsOnTheBus;
    }

    public String getHasContractorNotifiedSchools() {
        return hasContractorNotifiedSchools;
    }

    public String getGetHasContractorNotifiedParents() {
        return getHasContractorNotifiedParents;
    }

    public String getHaveYouAlertedOPT() {
        return haveYouAlertedOPT;
    }

    public DateTime getInformedOn() {
        return informedOn;
    }

    public String getIncidentNumber() {
        return incidentNumber;
    }

    public String getLastUpdatedOn() {
        return lastUpdatedOn;
    }

    public String getBreakdownOrRunningLate() {
        return breakdownOrRunningLate;
    }

    public String getSchoolAgeOrPrek() {
        return schoolAgeOrPrek;
    }

    public int getFlag() {
        return flag;
    }

    public int getRank() {
        return rank;
    }

    public void setSchoolYear(String schoolYear) {
        this.schoolYear = schoolYear;
    }

    public void setBusbreakdownId(int busbreakdownId) {
        this.busbreakdownId = busbreakdownId;
    }

    public void setRunType(String runType) {
        this.runType = runType;
    }

    public void setBusNo(String busNo) {
        this.busNo = busNo;
    }

    public void setRouteNumber(String routeNumber) {
        this.routeNumber = routeNumber;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public void setSchoolsServiced(String schoolsServiced) {
        this.schoolsServiced = schoolsServiced;
    }

    public void setOccurredOn(long occurredOn) {
        this.occurredOn = occurredOn;
    }

    public void setCreatedOn(DateTime createdOn) {
        this.createdOn = createdOn;
    }

    public void setBoro(String boro) {
        this.boro = boro;
    }

    public void setBusCompanyName(String busCompanyName) {
        this.busCompanyName = busCompanyName;
    }

    public void setHowLongDelayed(int howLongDelayed) {
        this.howLongDelayed = howLongDelayed;
    }

    public void setNumberOfStudentsOnTheBus(int numberOfStudentsOnTheBus) {
        this.numberOfStudentsOnTheBus = numberOfStudentsOnTheBus;
    }

    public void setHasContractorNotifiedSchools(String hasContractorNotifiedSchools) {
        this.hasContractorNotifiedSchools = hasContractorNotifiedSchools;
    }

    public void setGetHasContractorNotifiedParents(String getHasContractorNotifiedParents) {
        this.getHasContractorNotifiedParents = getHasContractorNotifiedParents;
    }

    public void setHaveYouAlertedOPT(String haveYouAlertedOPT) {
        this.haveYouAlertedOPT = haveYouAlertedOPT;
    }

    public void setInformedOn(DateTime informedOn) {
        this.informedOn = informedOn;
    }

    public void setIncidentNumber(String incidentNumber) {
        this.incidentNumber = incidentNumber;
    }

    public void setLastUpdatedOn(String lastUpdatedOn) {
        this.lastUpdatedOn = lastUpdatedOn;
    }

    public void setBreakdownOrRunningLate(String breakdownOrRunningLate) {
        this.breakdownOrRunningLate = breakdownOrRunningLate;
    }

    public void setSchoolAgeOrPrek(String schoolAgeOrPrek) {
        this.schoolAgeOrPrek = schoolAgeOrPrek;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }
}
