package model;

import java.io.Serializable;
import java.util.Date;

public class ArticleInfo implements Serializable {

    private String url;
    private String title;
    private Date sendDate;
    private String language;
    private String sourceCountry;
    private String imagePath;
    private String textPath;

    public ArticleInfo(String url, String title, Date sendDate, String language, String sourceCountry, String imagePath, String textPath) {
        this.url = url;
        this.title = title;
        this.sendDate = sendDate;
        this.language = language;
        this.sourceCountry = sourceCountry;
        this.imagePath = imagePath;
        this.textPath = textPath;
    }

    public String getUrl() {
        return url;
    }

    public String getTitle() {
        return title;
    }

    public Date getSendDate() {
        return sendDate;
    }

    public String getLanguage() {
        return language;
    }

    public String getSourceCountry() {
        return sourceCountry;
    }

    public String getImagePath() {
        return imagePath;
    }

    public String getTextPath() {
        return textPath;
    }
}
