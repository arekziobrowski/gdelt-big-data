import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.*;



public class ImagePixelProcessor {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Image processing");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> csv = sc.textFile("src/main/resources/color_metadata.csv");
        final JavaPairRDD<Color, Integer> lookupColor = csv.mapToPair(new PairFunction<String, Color, Integer>() {
            @Override
            public Tuple2<Color, Integer> call(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<>(
                        new Color(
                                Integer.parseInt(split[1]),
                                Integer.parseInt(split[2]),
                                Integer.parseInt(split[3])),
                        Integer.parseInt(split[0]));
            }
        });

        final Broadcast<Map<Color, Integer>> b = sc.broadcast(lookupColor.collectAsMap());


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

                            Integer id = b.value().get(c);

                            if (colorMap.containsKey(c)) {
                                colorMap.get(c).incrementCount();
                            }
                            else {
                                colorMap.put(c, new ImageMetadata(c, id, 1, new Date()));
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
