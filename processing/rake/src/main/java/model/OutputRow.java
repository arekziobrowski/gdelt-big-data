package model;

import java.io.Serializable;
import java.util.Objects;

public class OutputRow implements Serializable {
    private static final long serialVersionUID = 1L;

    private String articleId;
    private String phrase;
    private Double score;
    private String loadDate;

    public OutputRow(String articleId, String phrase, Double score, String loadDate) {
        this.articleId = articleId;
        this.phrase = phrase;
        this.score = score;
        this.loadDate = loadDate;
    }

    public String getArticleId() {
        return articleId;
    }

    public void setArticleId(String articleId) {
        this.articleId = articleId;
    }

    public String getPhrase() {
        return phrase;
    }

    public void setPhrase(String phrase) {
        this.phrase = phrase;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public String getLoadDate() {
        return loadDate;
    }

    public void setLoadDate(String loadDate) {
        this.loadDate = loadDate;
    }

    @Override
    public String toString() {
        return "null" + '\t' + Objects.toString(phrase, "") + '\t' + Objects.toString(score, "") + '\t'
                + Objects.toString(articleId, "") + '\t' + Objects.toString(loadDate, "");
    }
}
