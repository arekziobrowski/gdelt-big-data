package model;

import java.io.Serializable;
import java.util.Objects;

public class JsonEntryProcessed implements Serializable {
    private static final long serialVersionUID = 1L;

    private String url;
    private String title;
    private String seenDate;
    private String language;
    private String sourceCountry;
    private String imagePath;
    private String textPath;

    public JsonEntryProcessed() {
    }

    public String getLanguage() {
        return language;
    }

    public JsonEntryProcessed withLanguage(String language) {
        this.language = language;
        return this;
    }

    public String getSeenDate() {
        return seenDate;
    }

    public JsonEntryProcessed withSeenDate(String seendate) {
        this.seenDate = seendate;
        return this;
    }


    public String getSourceCountry() {
        return sourceCountry;
    }

    public JsonEntryProcessed withSourceCountry(String sourcecountry) {
        this.sourceCountry = sourcecountry;
        return this;
    }

    public String getTitle() {
        return title;
    }

    public JsonEntryProcessed withTitle(String title) {
        this.title = title;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public JsonEntryProcessed withUrl(String url) {
        this.url = url;
        return this;
    }

    public String getImagePath() {
        return imagePath;
    }

    public JsonEntryProcessed withImagePath(String imagePath) {
        this.imagePath = imagePath;
        return this;
    }

    public String getTextPath() {
        return textPath;
    }

    public JsonEntryProcessed withTextPath(String textPath) {
        this.textPath = textPath;
        return this;
    }

    @Override
    public String toString() {
        return Objects.toString(url, "") + '\t' + Objects.toString(title, "") + '\t'
                + Objects.toString(seenDate, "") + '\t' + Objects.toString(language, "") + '\t'
                + Objects.toString(sourceCountry, "") + '\t' + Objects.toString(imagePath, "")
                + '\t' + Objects.toString(textPath, "");
    }
}
