package functions;

import models.ArticleData;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Arrays;

public class ArticleDataOperations {

    public static class MapArticleDataToPairFunction implements PairFunction<ArticleData, String, ArticleData> {
        @Override
        public Tuple2<String, ArticleData> call(ArticleData inArticleData) {
            return new Tuple2<>(inArticleData.getSourceUrl(), inArticleData);
        }
    }

    public static class ArticleDataNotNullFilter implements Function<ArticleData, Boolean> {
        @Override
        public Boolean call(ArticleData inArticleData) {
            return !(inArticleData == null);
        }
    }

    public static class MapArticleDataToArticleData implements Function<String, ArticleData> {
        @Override
        public ArticleData call(String s) {
            String[] spl = s.split("\t");
            ArticleData articleData = null;

            try {
                articleData = new ArticleData(
                        spl[0],
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(spl[1]),
                        new SimpleDateFormat("yyyy-MM-dd").parse(spl[2]),
                        spl[3],
                        Double.parseDouble(spl[8]),
                        spl[11]);
            } catch (Exception e) {
                return null;
            }

            return articleData;
        }
    }
}
