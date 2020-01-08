import functions.*;
import model.ArticleInfo;
import model.Color;
import model.Image;
import model.ImageMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import redis.clients.jedis.Jedis;
import utils.Util;


import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.util.*;



public class ImagePixelProcessor {

    private static String ARTICLE_LOOKUP_PATH = "/etl/staging/load/{RUN_CONTROL_DATE}/article_lookup.dat";
    private static String ARTICLES_API_INFO_CLEANSED_PATH = "/etl/staging/cleansed/{RUN_CONTROL_DATE}/api/articles-api-info-cleansed.dat";
    private static final Integer PARTITIONS_NUMBER = 4;

    private static String IMAGE_METADATA_OUTPUT_PATH = "/etl/staging/load/{RUN_CONTROL_DATE}/image_metadata.dat";
    private static String IMAGE_OUTPUT_PATH = "/etl/staging/load/{RUN_CONTROL_DATE}/image.dat";

    private static final String RUN_CONTROL_DATE_PLACEHOLDER = "{RUN_CONTROL_DATE}";



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

        System.out.println(Util.getJedis().getResource().get("0,0,0"));

        final Configuration hc = Util.getSc().hadoopConfiguration();

        FileSystem fileSystem = FileSystem.get(hc);

        fileSystem.delete(new Path(IMAGE_METADATA_OUTPUT_PATH), true);
        fileSystem.delete(new Path(IMAGE_OUTPUT_PATH), true);
        fileSystem.close();

        JavaPairRDD<String, Integer> articleLookup =
            Util.getSc().textFile(ARTICLE_LOOKUP_PATH)
                .mapToPair(new ArticleLookupSplitPairFunction())
                .partitionBy(new HashPartitioner(PARTITIONS_NUMBER));

        final Broadcast<Map<String, Integer>> b = Util.getSc().broadcast(articleLookup.collectAsMap());

        JavaRDD<ArticleInfo> articleInfos =
            Util.getSc().textFile(ARTICLES_API_INFO_CLEANSED_PATH)
            .map(new ArticleInfoSplitFunction())
            .filter(new Function<ArticleInfo, Boolean>() {
                @Override
                public Boolean call(ArticleInfo articleInfo) throws Exception {
                    return articleInfo.getImagePath() != null && !articleInfo.getImagePath().isEmpty();
                }
            })
            .cache();

        JavaRDD<Image> images = articleInfos.map(new ImageMapFunction(b));

        images.saveAsTextFile(IMAGE_OUTPUT_PATH);

        JavaRDD<ImageMetadata> ims =
                articleInfos
                        .flatMap(new ImageMetadataComputeFlatMapFunction(b))
                        .filter(new ImageMetadataFilterFunction());

        ims.saveAsTextFile(IMAGE_METADATA_OUTPUT_PATH);
        Util.getSc().close();
    }

    private static String trimQuotes(String s) {
        return s.substring(1, s.length() - 1);
    }
}
