package functions;

import model.ArticleInfo;
import model.Color;
import model.ImageMetadata;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import redis.clients.jedis.Jedis;
import utils.Util;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ImageMetadataComputeFlatMapFunction implements FlatMapFunction<ArticleInfo, ImageMetadata> {

    private final Broadcast<Map<String, Integer>> b;

    public ImageMetadataComputeFlatMapFunction(Broadcast<Map<String, Integer>> b) {
        this.b = b;
    }

    @Override
    public Iterable<ImageMetadata> call(ArticleInfo articleInfo) throws Exception {
        HashMap<Color, ImageMetadata> colorMap = new HashMap<>();

        FileSystem fileSystem = FileSystem.newInstance(Util.getSc().hadoopConfiguration());
        FSDataInputStream fs = fileSystem.open(new Path(articleInfo.getImagePath()));
        BufferedImage image = ImageIO.read(fs);


        if (image != null) {
            for (int x = 0; x < image.getWidth(); x++) {
                for (int y = 0; y < image.getHeight(); y++) {
                    int color = image.getRGB(x, y);
                    int red = (color & 0x00ff0000) >> 16;
                    int green = (color & 0x0000ff00) >> 8;
                    int blue = color & 0x000000ff;
                    Color c = new Color(red, green, blue);

                    if (colorMap.containsKey(c)) {
                        colorMap.get(c).incrementCount();
                    }
                    else {
                        Jedis jedis = Util.getJedis().getResource();
                        colorMap.put(c, new ImageMetadata(Integer.parseInt(jedis.get(c.toString())), b.value().get(articleInfo.getUrl()), new Date()));
                        jedis.close();
                    }
                }
            }
        }
        fileSystem.close();
        return colorMap.values();
    }
}
