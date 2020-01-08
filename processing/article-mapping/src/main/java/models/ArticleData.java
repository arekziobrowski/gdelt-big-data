package models;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

public class ArticleData implements Serializable {

    private String sourceUrl;   // key in1
    private Date dateAdded;
    private Date eventDate;
    private String eventCode;       // key in2 - topic
    private int averageTone;
    private String countryCode;


    public ArticleData(String sourceUrl, Date dateAdded, Date eventDate, String eventCode, int averageTone, String countryCode) {
        this.sourceUrl = sourceUrl;
        this.dateAdded = dateAdded;
        this.eventDate = eventDate;
        this.eventCode = eventCode;
        this.averageTone = averageTone;
        this.countryCode = countryCode;
    }

    @Override
    public String toString() {
        return Objects.toString(sourceUrl, "") + '\t'
                + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dateAdded) + '\t'
                + new SimpleDateFormat("yyyy-MM-dd").format(eventDate) + '\t'
                + Objects.toString(eventCode, "") + '\t'
                + Objects.toString(averageTone, "") + '\t'
                + Objects.toString(countryCode, "");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        ArticleData o = (ArticleData) obj;
        return Objects.equals(sourceUrl, o.sourceUrl) &&
                Objects.equals(dateAdded, o.dateAdded) &&
                Objects.equals(eventDate, o.eventDate) &&
                Objects.equals(eventCode, o.eventCode) &&
                Objects.equals(averageTone, o.averageTone) &&
                Objects.equals(countryCode, o.countryCode);
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
