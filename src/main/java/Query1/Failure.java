package Query1;

import org.joda.time.DateTime;

public class Failure {

    private String schoolYear;
    private int busbreakdownId;
    private String runType;
    private String busNo;
    private String routeNumber;
    private String reason;
    private String schoolsServiced;
    private DateTime occurredOn;
    private DateTime createdOn;
    private String boro;
    private String busCompanyName;
    private String howLongDelayed;
    private int numberOfStudentsOnTheBus;
    private String hasContractorNotifiedSchools;
    private String getHasContractorNotifiedParents;
    private String haveYouAlertedOPT;
    private DateTime informedOn;
    private String incidentNumber;
    private String lastUpdatedOn;
    private String breakdownOrRunningLate;
    private String schoolAgeOrPrek;

    public Failure(String schoolYear, int busbreakdownId, String runType, String busNo, String routeNumber,
                   String reason, String schoolsServiced, DateTime occurredOn, DateTime createdOn,
                   String boro, String busCompanyName, String howLongDelayed, int numberOfStudentsOnTheBus,
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

    public Failure(DateTime occurredOn, String boro, String howLongDelayed) {
        this.occurredOn = occurredOn;
        this.boro = boro;
        this.howLongDelayed = howLongDelayed;
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

    public DateTime getOccurredOn() {
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

    public String getHowLongDelayed() {
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
}
