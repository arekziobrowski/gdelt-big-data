package model;

import java.io.Serializable;

public class JsonEntry implements Serializable {
    private static final long serialVersionUID = 1L;

    private String domain;
    private String language;
    private String seenDate;
    private String socialImage;
    private String sourceCountry;
    private String title;
    private String url;
    private String urlMobile;

    private String articlePath;
    private String imagePath;

    public JsonEntry() {
    }


    public String getDomain() {
        return domain;
    }

    public JsonEntry withDomain(String domain) {
        this.domain = domain;
        return this;
    }

    public String getLanguage() {
        return language;
    }

    public JsonEntry withLanguage(String language) {
        this.language = language;
        return this;
    }

    public String getSeenDate() {
        return seenDate;
    }

    public JsonEntry withSeenDate(String seenDate) {
        this.seenDate = seenDate;
        return this;
    }

    public String getSocialImage() {
        return socialImage;
    }

    public JsonEntry withSocialImage(String socialimage) {
        this.socialImage = socialimage;
        return this;
    }

    public String getSourceCountry() {
        return sourceCountry;
    }

    public JsonEntry withSourceCountry(String sourceCountry) {
        this.sourceCountry = sourceCountry;
        return this;
    }

    public String getTitle() {
        return title;
    }

    public JsonEntry withTitle(String title) {
        this.title = title;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public JsonEntry withUrl(String url) {
        this.url = url;
        return this;
    }

    public String getUrlMobile() {
        return urlMobile;
    }

    public JsonEntry withUrlMobile(String url_mobile) {
        this.urlMobile = url_mobile;
        return this;
    }

    public String getArticlePath() {
        return articlePath;
    }

    public JsonEntry withArticlePath(String articlePath) {
        this.articlePath = articlePath;
        return this;
    }

    public String getImagePath() {
        return imagePath;
    }

    public JsonEntry withImagePath(String imagePath) {
        this.imagePath = imagePath;
        return this;
    }
}
