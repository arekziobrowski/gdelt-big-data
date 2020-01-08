package models;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

public class ArticleApiInfoAndArticleData implements Serializable {

    private String title;
    private String url;
    private Date datePublished;
    private Date dateEvent;
    private String eventCode;       // key in2 - topic
    private String language;
    private int tone;
    private String countryId;

    public ArticleApiInfoAndArticleData(String title, String url, Date datePublished, Date dateEvent, String eventCode,
                                        String language, int tone, String countryId) {
        this.title = title;
        this.url = url;
        this.datePublished = datePublished;
        this.dateEvent = dateEvent;
        this.eventCode = eventCode;
        this.language = language;
        this.tone = tone;
        this.countryId = countryId;
    }

    @Override
    public String toString() {
        return Objects.toString(title, "") + '\t'
                + Objects.toString(url, "") + '\t'
                + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(datePublished) + '\t'
                + new SimpleDateFormat("yyyy-MM-dd").format(dateEvent) + '\t'
                + Objects.toString(eventCode, "") + '\t'
                + Objects.toString(language, "") + '\t'
                + Objects.toString(tone, "") + '\t'
                + Objects.toString(countryId, "");
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Date getDatePublished() {
        return datePublished;
    }

    public void setDatePublished(Date datePublished) {
        this.datePublished = datePublished;
    }

    public Date getDateEvent() {
        return dateEvent;
    }

    public void setDateEvent(Date dateEvent) {
        this.dateEvent = dateEvent;
    }

    public String getEventCode() {
        return eventCode;
    }

    public void setEventCode(String eventCode) {
        this.eventCode = eventCode;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public int getTone() {
        return tone;
    }

    public void setTone(int tone) {
        this.tone = tone;
    }

    public String getCountryId() {
        return countryId;
    }

    public void setCountryId(String countryId) {
        this.countryId = countryId;
    }
}
