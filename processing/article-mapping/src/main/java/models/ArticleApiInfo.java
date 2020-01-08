package models;

import java.io.Serializable;
import java.util.Objects;

public class ArticleApiInfo implements Serializable {

    private String url;
    private String title;
    private String language;

    public ArticleApiInfo(String url, String title, String language) {
        this.url = url;
        this.title = title;
        this.language = language;
    }

    @Override
    public String toString() {
        return Objects.toString(url, "") + '\t'
                + Objects.toString(title, "") + '\t'
                + Objects.toString(language, "");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        ArticleApiInfo o = (ArticleApiInfo) obj;
        return Objects.equals(url, o.url) &&
                Objects.equals(title, o.title) &&
                Objects.equals(language, o.language);
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }
}
