package functions;

import models.Article;
import models.ArticleApiInfoAndArticleData;
import models.EventCode;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class MapArticleApiInfoAndArticleDataAndEventCodes implements Function<Tuple2<ArticleApiInfoAndArticleData, EventCode>, Article> {
    @Override
    public Article call(Tuple2<ArticleApiInfoAndArticleData, EventCode> inTuple2) {
        return new Article(
                inTuple2._1.getTitle(),
                inTuple2._1.getUrl(),
                inTuple2._1.getDatePublished(),
                inTuple2._1.getDateEvent(),
                inTuple2._2.getEventDescription(),
                inTuple2._1.getLanguage(),
                inTuple2._1.getTone(),
                inTuple2._1.getCountryId());
    }
}
