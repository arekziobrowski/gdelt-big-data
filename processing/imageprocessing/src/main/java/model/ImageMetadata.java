package model;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

public class ImageMetadata implements Serializable {

    private Integer count;
    private Integer colorId;
    private Integer articleId;
    private Date loadDate;

    public ImageMetadata(Integer colorId, Integer articleId, Date loadDate) {
        this.count = 1;
        this.colorId = colorId;
        this.articleId = articleId;
        this.loadDate = loadDate;
    }

    @Override
    public String toString() {
        return Objects.toString(count, "")
                + '\t' + Objects.toString(colorId, "")
                + '\t' + Objects.toString(articleId, "")
                + '\t' + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(loadDate);
    }

    public void incrementCount() {
        this.count++;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Integer getColorId() {
        return colorId;
    }

    public void setColorId(Integer colorId) {
        this.colorId = colorId;
    }

    public Integer getArticleId() {
        return articleId;
    }

    public void setArticleId(Integer articleId) {
        this.articleId = articleId;
    }

    public Date getLoadDate() {
        return loadDate;
    }

    public void setLoadDate(Date loadDate) {
        this.loadDate = loadDate;
    }
}
