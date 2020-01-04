package functions;

import com.roxstudio.utils.CUrl;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import model.JsonEntry;
import org.apache.hadoop.io.MD5Hash;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;

public class TextDownloadMapFunction implements PairFunction<JsonEntry, JsonEntry, String> {
    private final String prefixPath;

    public TextDownloadMapFunction(String prefixPath) {
        this.prefixPath = prefixPath;
    }

    @Override
    public Tuple2<JsonEntry, String> call(JsonEntry v1) throws Exception {
        String content = "";
        try {
            URL url = new URL(v1.getUrl());
            CUrl curl = new CUrl(v1.getUrl()).timeout(0.5f, 1.0f);
            byte[] z = curl.exec();
            InputStream is = new ByteArrayInputStream(z);

            ArticleExtractor articleExtractor = ArticleExtractor.getInstance();
            content = articleExtractor.getText(new InputStreamReader(is));
            if (content.length() != 0) {
                System.out.println(content.length());
                v1.withArticlePath(prefixPath + MD5Hash.digest(v1.getTitle().getBytes()).toString());
            }
        } catch (MalformedURLException ex) {
            System.out.println("Invalid URL: " + v1.getUrl());
        }
        return new Tuple2<>(v1, content);
    }
}
