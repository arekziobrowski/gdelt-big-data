import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.net.URI;
import java.util.*;



public class ImagePixelProcessor {

    public static void main(String[] args) throws Exception {
        
        SparkConf conf =
                new SparkConf()
                .setMaster("local[2]")
                .setAppName("Image processing");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> paths = sc.parallelize(Arrays.asList("src/main/resources/test"));
        JavaRDD<ImageMetadata> ims = paths.flatMap(new FlatMapFunction<String, ImageMetadata>() {
            @Override
            public Iterable<ImageMetadata> call(String s) throws Exception {
                HashMap<Color, ImageMetadata> colorMap = new HashMap<>();
                BufferedImage image = ImageIO.read(new File(s));

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
                                colorMap.put(c, new ImageMetadata(c, Integer.parseInt(Util.getJedis().get(c.toString())), 1, new Date()));
                            }
                        }
                    }
                }
                return colorMap.values();
            }
        });



        for (ImageMetadata i : ims.collect()) {
            //Color c = i.getC();
            System.out.println("ID: " + i.getColorId());
            //System.out.println("R:" + c.getR() + ", G: " + c.getG() + ", B: " + c.getB() + ", count: " + i.getCount());
        }

    }
}
