import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.HashPartitioner;
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

    private static String ARTICLE_LOOKUP_PATH = "/etl/staging/load/{RUN_CONTROL_DATE}/article_lookup.dat";
    private static String ARTICLES_API_INFO_CLEANSED_PATH = "/etl/staging/cleansed/{RUN_CONTROL_DATE}/api/articles-api-info-cleansed.dat";
    private static final Integer PARTITIONS_NUMBER = 4;

    private static String IMAGE_METADATA_OUTPUT_PATH = "/etl/staging/load/{RUN_CONTROL_DATE}/image_metadata.dat";
    private static String IMAGE_OUTPUT_PATH = "/etl/staging/load/{RUN_CONTROL_DATE}/image.dat";

    private static final String RUN_CONTROL_DATE_PLACEHOLDER = "{RUN_CONTROL_DATE}";
    private static final String DELIMITER = "\t";

    private static final JavaSparkContext sc = new JavaSparkContext(new SparkConf()
            .setMaster("local[2]")
            .setAppName("Image processing"));

    public static void main(String[] args) throws Exception {

        if (args.length == 0 || !args[0].matches("[0-9]{8}")) {
            System.err.println("Provide RUN_CONTROL_DATE with args[].");
            System.exit(2);
        }
        final String RUN_CONTROL_DATE = args[0];

        ARTICLE_LOOKUP_PATH = ARTICLE_LOOKUP_PATH.replace(RUN_CONTROL_DATE_PLACEHOLDER, RUN_CONTROL_DATE);
        ARTICLES_API_INFO_CLEANSED_PATH = ARTICLES_API_INFO_CLEANSED_PATH.replace(RUN_CONTROL_DATE_PLACEHOLDER, RUN_CONTROL_DATE);

        IMAGE_METADATA_OUTPUT_PATH = IMAGE_METADATA_OUTPUT_PATH.replace(RUN_CONTROL_DATE_PLACEHOLDER, RUN_CONTROL_DATE);
        IMAGE_OUTPUT_PATH = IMAGE_OUTPUT_PATH.replace(RUN_CONTROL_DATE_PLACEHOLDER, RUN_CONTROL_DATE);



        final Configuration hc = sc.hadoopConfiguration();

        FileSystem fileSystem = FileSystem.get(hc);

        fileSystem.delete(new Path(IMAGE_METADATA_OUTPUT_PATH), true);
        fileSystem.delete(new Path(IMAGE_OUTPUT_PATH), true);
        fileSystem.close();

        JavaPairRDD<String, Integer> articleLookup =
            sc.textFile(ARTICLE_LOOKUP_PATH)
                .mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        String[] spl = s.split(DELIMITER);
                        return new Tuple2<>(spl[1], Integer.parseInt(spl[0]));
                    }
                }).partitionBy(new HashPartitioner(PARTITIONS_NUMBER));

        final Broadcast<Map<String, Integer>> b = sc.broadcast(articleLookup.collectAsMap());

        JavaRDD<ArticleInfo> articleInfos =
            sc.textFile(ARTICLES_API_INFO_CLEANSED_PATH)
            .map(new Function<String, ArticleInfo>() {
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
            }).cache();

        JavaRDD<Image> images = articleInfos.map(new Function<ArticleInfo, Image>() {
            @Override
            public Image call(ArticleInfo articleInfo) throws Exception {
                return new Image(null, articleInfo.getImagePath(), b.value().get(articleInfo.getUrl()), new Date());
            }
        });

        images.saveAsTextFile(IMAGE_OUTPUT_PATH);

        JavaRDD<ImageMetadata> ims = articleInfos.flatMap(new FlatMapFunction<ArticleInfo, ImageMetadata>() {
            @Override
            public Iterable<ImageMetadata> call(ArticleInfo articleInfo) throws Exception {
                HashMap<Color, ImageMetadata> colorMap = new HashMap<>();
                // BufferedImage image = ImageIO.read(new File(articleInfo.getImagePath()));

                FileSystem fileSystem = FileSystem.get(sc.hadoopConfiguration());
                BufferedImage image = ImageIO.read(fileSystem.open(new Path(articleInfo.getImagePath())));


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
                                colorMap.put(c, new ImageMetadata(Integer.parseInt(Util.getJedis().getResource().get(c.toString())), b.value().get(articleInfo.getUrl()), new Date()));
                            }
                        }
                    }
                }
                fileSystem.close();
                return colorMap.values();
            }
        });

        ims.saveAsTextFile(IMAGE_METADATA_OUTPUT_PATH);
        sc.close();
    }

    private static String trimQuotes(String s) {
        return s.substring(1, s.length() - 1);
    }
}
