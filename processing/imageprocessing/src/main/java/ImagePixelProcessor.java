import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;


import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;



public class ImagePixelProcessor {

    private static final String ARTICLE_LOOKUP_PATH = "src/main/resources/article_lookup.dat";
    private static final String ARTICLES_API_INFO_CLEANSED_PATH = "src/main/resources/articles-api-info-cleansed.csv";

    public static void main(String[] args) throws Exception {

        SparkConf conf =
                new SparkConf()
                .setMaster("local[2]")
                .setAppName("Image processing");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> articleLookup =
            sc.textFile(ARTICLE_LOOKUP_PATH)
                .mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        String[] spl = s.split("\\|");
                        return new Tuple2<>(trimQuotes(spl[1]), Integer.parseInt(spl[0]));
                    }
                });

        final Broadcast<Map<String, Integer>> b = sc.broadcast(articleLookup.collectAsMap());

        JavaRDD<ArticleInfo> articleInfos =
            sc.textFile(ARTICLES_API_INFO_CLEANSED_PATH)
            .map(new Function<String, ArticleInfo>() {
                @Override
                public ArticleInfo call(String s) throws Exception {
                    String[] spl = s.split("\\|");
                    return new ArticleInfo(
                            trimQuotes(spl[0]),
                            trimQuotes(spl[1]),
                            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(trimQuotes(spl[2])),
                            trimQuotes(spl[3]),
                            trimQuotes(spl[4]),
                            trimQuotes(spl[5]),
                            trimQuotes(spl[6])
                    );
                }
            }).cache();



        JavaRDD<Image> images = articleInfos.map(new Function<ArticleInfo, Image>() {
            @Override
            public Image call(ArticleInfo articleInfo) throws Exception {
                return new Image(null, articleInfo.getImagePath(), b.value().get(articleInfo.getUrl()), new Date());
            }
        });

        for(Image image : images.collect()) {
            System.out.println(image);
        }
        
        JavaRDD<ImageMetadata> ims = articleInfos.flatMap(new FlatMapFunction<ArticleInfo, ImageMetadata>() {
            @Override
            public Iterable<ImageMetadata> call(ArticleInfo articleInfo) throws Exception {
                HashMap<Color, ImageMetadata> colorMap = new HashMap<>();
                BufferedImage image = ImageIO.read(new File(articleInfo.getImagePath()));

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



        /*for (ImageMetadata i : ims.collect()) {
            //Color c = i.getC();
            System.out.println("ID: " + i.getColorId());
            //System.out.println("R:" + c.getR() + ", G: " + c.getG() + ", B: " + c.getB() + ", count: " + i.getCount());
        }*/

    }

    private static String trimQuotes(String s) {
        return s.substring(1, s.length() - 1);
    }
}
