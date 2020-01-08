package functions;

import models.ArticleApiInfo;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class ArticleApiInfoOperations {

    public static class MapArticleApiInfoToPairFunction implements PairFunction<ArticleApiInfo, String, ArticleApiInfo> {
        @Override
        public Tuple2<String, ArticleApiInfo> call(ArticleApiInfo inArticleApiInfo) {
            return new Tuple2<>(inArticleApiInfo.getUrl(), inArticleApiInfo);
        }
    }

    public static class ArticleApiInfoNotNullFilter implements Function<ArticleApiInfo, Boolean> {
        @Override
        public Boolean call(ArticleApiInfo articleApiInfo) {
            return !(articleApiInfo == null);
        }
    }

    public static class MapArticleApiInfoToArticleApiInfo implements Function<String, ArticleApiInfo> {
        @Override
        public ArticleApiInfo call(String s) {
            String[] spl = s.split("\t", 7);
            ArticleApiInfo articleApiInfo = null;

            try {
                articleApiInfo = new ArticleApiInfo(
                        spl[0],
                        spl[1],
                        spl[3]);
            } catch (Exception e) {
                return null;
            }

            return articleApiInfo;
        }
    }
}
