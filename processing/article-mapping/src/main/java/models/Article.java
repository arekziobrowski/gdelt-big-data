package models;

import java.io.Serializable;
import java.util.Date;

public class Article implements Serializable {

    // ArticleApiInfo title
    private String title;
    // ArticleApiInfo url
    private String url;
    // ArticleData dateAdded
    private Date datePublished;
    // ArticleData eventDate
    private Date dateEvent;
    // EventCode eventDescription
    private String topic;
    // ArticleApiInfo language
    private String language;
    // ArticleData averageTone
    private String tone;
    // ArticleData countryCode
    private String countryId;
    // now()
    private Date loadDate;

    public Article(String title, String url, Date datePublished, Date dateEvent, String topic,
                   String language, String tone, String countryId) {
        this.title = title;
        this.datePublished = datePublished;
        this.dateEvent = dateEvent;
        this.topic = topic;
        this.language = language;
        this.tone = tone;
        this.countryId = countryId;
        this.loadDate = new Date();
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

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getTone() {
        return tone;
    }

    public void setTone(String tone) {
        this.tone = tone;
    }

    public String getCountryId() {
        return countryId;
    }

    public void setCountryId(String countryId) {
        this.countryId = countryId;
    }

    public Date getLoadDate() {
        return loadDate;
    }

    public void setLoadDate(Date loadDate) {
        this.loadDate = loadDate;
    }

}
