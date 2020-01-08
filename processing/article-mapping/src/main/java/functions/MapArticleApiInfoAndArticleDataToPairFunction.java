package functions;

import models.ArticleApiInfo;
import models.ArticleApiInfoAndArticleData;
import models.ArticleData;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class MapArticleApiInfoAndArticleDataToPairFunction implements PairFunction<Tuple2<ArticleApiInfo, ArticleData>, String, ArticleApiInfoAndArticleData> {
    @Override
    public Tuple2<String, ArticleApiInfoAndArticleData> call(Tuple2<ArticleApiInfo, ArticleData> inTuple2) {
        ArticleApiInfoAndArticleData articleApiInfoAndArticleData = new ArticleApiInfoAndArticleData(
                inTuple2._1.getTitle(),
                inTuple2._1.getUrl(),
                inTuple2._2.getDateAdded(),
                inTuple2._2.getEventDate(),
                inTuple2._2.getEventCode(),
                inTuple2._1.getLanguage(),
                inTuple2._2.getAverageTone(),
                inTuple2._2.getCountryCode());

        return new Tuple2<>(inTuple2._2.getEventCode(), articleApiInfoAndArticleData);
    }
}