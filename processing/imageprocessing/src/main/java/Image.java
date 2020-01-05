import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Image implements Serializable {

    private String id;
    private String url;
    private Integer article_id;
    private Date loadDate;

    public Image(String id, String url, Integer article_id, Date loadDate) {
        this.id = id;
        this.url = url;
        this.article_id = article_id;
        this.loadDate = loadDate;
    }

    @Override
    public String toString() {
        return id + '\t' + url + '\t' + article_id + "\t" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(loadDate);
    }

    public String getId() {
        return id;
    }

    public String getUrl() {
        return url;
    }

    public Integer getArticle_id() {
        return article_id;
    }

    public Date getLoadDate() {
        return loadDate;
    }
}
