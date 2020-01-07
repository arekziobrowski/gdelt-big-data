package functions;

import model.ArticleInfo;
import org.apache.spark.api.java.function.Function;

import java.text.SimpleDateFormat;

public class ArticleInfoSplitFunction implements Function<String, ArticleInfo> {

    private static final String DELIMITER = "\t";

    @Override
    public ArticleInfo call(String s) throws Exception {
        String[] spl = s.split(DELIMITER);
        return new ArticleInfo(
                spl[0],
                spl[1],
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(spl[2]),
                spl[3],
                spl[4],
                spl[5],
                spl[6]
        );
    }
}
