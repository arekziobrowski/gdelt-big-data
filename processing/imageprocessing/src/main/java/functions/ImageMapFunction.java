package functions;

import model.ArticleInfo;
import model.Image;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.util.Date;
import java.util.Map;

public class ImageMapFunction implements Function<ArticleInfo, Image> {

    private final Broadcast<Map<String, Integer>> b;

    public ImageMapFunction(Broadcast<Map<String, Integer>> b) {
        this.b = b;
    }

    @Override
    public Image call(ArticleInfo articleInfo) throws Exception {
        return new Image(null, articleInfo.getImagePath(), b.value().get(articleInfo.getUrl()), new Date());
    }
}
