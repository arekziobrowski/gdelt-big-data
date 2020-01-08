package models;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

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
    private int tone;
    // ArticleData countryCode
    private String countryId;
    // now()
    private Date loadDate;

    public Article(String title, String url, Date datePublished, Date dateEvent, String topic,
                   String language, int tone, String countryId) {
        this.title = title;
        this.url = url;
        this.datePublished = datePublished;
        this.dateEvent = dateEvent;
        this.topic = topic;
        this.language = language;
        this.tone = tone;
        this.countryId = countryId;
        this.loadDate = new Date();
    }

    @Override
    public String toString() {
        return Objects.toString(title, "") + '\t'
                + Objects.toString(url, "") + '\t'
                + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(datePublished) + '\t'
                + new SimpleDateFormat("yyyy-MM-dd").format(dateEvent) + '\t'
                + Objects.toString(topic, "") + '\t'
                + Objects.toString(language, "") + '\t'
                + Objects.toString(tone, "") + '\t'
                + Objects.toString(countryId, "") + '\t'
                + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(loadDate);
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

    public Date getLoadDate() {
        return loadDate;
    }

    public void setLoadDate(Date loadDate) {
        this.loadDate = loadDate;
    }

}
