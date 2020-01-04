package functions;

import model.JsonEntry;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class ImageDownloadMapFunction implements PairFunction<JsonEntry, JsonEntry, byte[]> {
    private final String prefixPath;
    private final int connectionTimeout;

    public ImageDownloadMapFunction(String prefixPath, int connectionTimeout) {
        this.prefixPath = prefixPath;
        this.connectionTimeout = connectionTimeout;
    }

    @Override
    public Tuple2<JsonEntry, byte[]> call(JsonEntry input) throws Exception {
        final String url = input.getSocialImage();
        byte[] content = null;
        try {
            final HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setReadTimeout(connectionTimeout * 1000);
            conn.setConnectTimeout(connectionTimeout * 1000);
            conn.setRequestMethod("GET");
            content = IOUtils.toByteArray(conn.getInputStream());
            System.out.println("I: " + content.length);
            input.withImagePath(prefixPath + MD5Hash.digest(input.getSocialImage().getBytes()).toString());
        } catch (IOException e) {
            System.out.println("Issue with downloading image from: " + url);
        }

        return new Tuple2<>(input, content);
    }
}
