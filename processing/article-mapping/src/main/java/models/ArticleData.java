package models;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

public class ArticleData implements Serializable {

    // key in1
    private String sourceUrl;
    private Date dateAdded;
    private Date eventDate;
    // key in2
    private String eventCode;       // topic
    //    private float goldsteinScale;
//    private int numMentions;
//    private int numArticles;
//    private int numSources;
    private int averageTone;
    //    private String geoType;
//    private String geoFullName;
    private String countryCode;

    public ArticleData(String sourceUrl, Date dateAdded, Date eventDate, int averageTone, String countryCode) {
        this.sourceUrl = sourceUrl;
        this.dateAdded = dateAdded;
        this.eventDate = eventDate;
        this.averageTone = averageTone;
        this.countryCode = countryCode;
    }

    @Override
    public String toString() {
        return Objects.toString(sourceUrl, "") + '\t'
                + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dateAdded) + '\t'
                + new SimpleDateFormat("yyyy-MM-dd").format(eventDate) + '\t'
                + Objects.toString(averageTone, "") + '\t'
                + Objects.toString(countryCode, "");
    }

    public Date getEventDate() {
        return eventDate;
    }

    public void setEventDate(Date eventDate) {
        this.eventDate = eventDate;
    }

    public String getSourceUrl() {
        return sourceUrl;
    }

    public void setSourceUrl(String sourceUrl) {
        this.sourceUrl = sourceUrl;
    }

    public Date getDateAdded() {
        return dateAdded;
    }

    public void setDateAdded(Date dateAdded) {
        this.dateAdded = dateAdded;
    }

    public int getAverageTone() {
        return averageTone;
    }

    public void setAverageTone(int averageTone) {
        this.averageTone = averageTone;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public String getEventCode() {
        return eventCode;
    }

    public void setEventCode(String eventCode) {
        this.eventCode = eventCode;
    }
}
