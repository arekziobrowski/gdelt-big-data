import functions.*;
import models.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;

public class ArticleMapping {
    private static final String OUTPUT_FILES_PREFIX = "article.dat-";
    private static final String RUN_CONTROL_DATE_PLACEHOLDER = "{RUN_CONTROL_DATE}";
    private static final Integer PARTITIONS_NUMBER = 4;
    private static String INPUT_FILES_ARTICLES_DATA_CLEANSED_PATTERN = "/etl/staging/cleansed/{RUN_CONTROL_DATE}/articles-data-cleansed.dat-*";
    private static String INPUT_FILE_ARTICLES_API_INFO_CLEANSED_PATTERN = "/etl/staging/cleansed/{RUN_CONTROL_DATE}/api/articles-api-info-cleansed.dat-*";
    private static String INPUT_FILE_CAMEO_EVENT_CODES = "/data/gdelt/{RUN_CONTROL_DATE}/cameo/CAMEO.eventcodes.txt";
    private static String OUT_DIR = "/etl/staging/load/{RUN_CONTROL_DATE}/article/";

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || !args[0].matches("[0-9]{8}")) {
            System.err.println("Provide RUN_CONTROL_DATE with args[].");
            System.exit(2);
        }
        final String RUN_CONTROL_DATE = args[0];

        //CONSTANTS
        INPUT_FILES_ARTICLES_DATA_CLEANSED_PATTERN = INPUT_FILES_ARTICLES_DATA_CLEANSED_PATTERN.replace(RUN_CONTROL_DATE_PLACEHOLDER, RUN_CONTROL_DATE);
        INPUT_FILE_ARTICLES_API_INFO_CLEANSED_PATTERN = INPUT_FILE_ARTICLES_API_INFO_CLEANSED_PATTERN.replace(RUN_CONTROL_DATE_PLACEHOLDER, RUN_CONTROL_DATE);
        INPUT_FILE_CAMEO_EVENT_CODES = INPUT_FILE_CAMEO_EVENT_CODES.replace(RUN_CONTROL_DATE_PLACEHOLDER, RUN_CONTROL_DATE);
        OUT_DIR = OUT_DIR.replace(RUN_CONTROL_DATE_PLACEHOLDER, RUN_CONTROL_DATE);

        // CONFIGURATION
        SparkConf conf = new SparkConf().setAppName("RAKE Processing");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Configuration hadoopConfiguration = sc.hadoopConfiguration();
        SQLContext sqlContext = new SQLContext(sc);

        // CLEANING ARTICLE.DAT* FILES
        FileSystem fileSystem = FileSystem.get(sc.hadoopConfiguration());
        fileSystem.delete(new Path(OUT_DIR), true);
        fileSystem.close();

        // PROCESSING
        JavaPairRDD<String, ArticleData> articleData = sc.textFile(INPUT_FILES_ARTICLES_DATA_CLEANSED_PATTERN)
                .repartition(PARTITIONS_NUMBER)
                .distinct()
                .map(new ArticleDataOperations.MapArticleDataToArticleData())
                .filter(new ArticleDataOperations.ArticleDataNotNullFilter())
                .mapToPair(new ArticleDataOperations.MapArticleDataToPairFunction())
                .cache();

        final Broadcast<JavaPairRDD<String, ArticleData>> broadcastArticleData = sc.broadcast(articleData);


        JavaPairRDD<String, EventCode> eventCodes = sc.textFile(INPUT_FILE_CAMEO_EVENT_CODES)
                .repartition(PARTITIONS_NUMBER)
                .distinct()
                .map(new EventCodeOperations.MapEventCodeToEventCode())
                .filter(new EventCodeOperations.EventCodeNotNullFilter())
                .mapToPair(new EventCodeOperations.MapEventCodeToPairFunction())
                .cache();

        final Broadcast<JavaPairRDD<String, EventCode>> broadcastCameoEventCodes = sc.broadcast(eventCodes);


        // API_INFO has the biggest amount of data, so it won't be broadcasted
        JavaPairRDD<String, ArticleApiInfo> articleApiInfo = sc.textFile(INPUT_FILE_ARTICLES_API_INFO_CLEANSED_PATTERN)
                .repartition(PARTITIONS_NUMBER)
                .distinct()
                .map(new ArticleApiInfoOperations.MapArticleApiInfoToArticleApiInfo())
                .filter(new ArticleApiInfoOperations.ArticleApiInfoNotNullFilter())
                .mapToPair(new ArticleApiInfoOperations.MapArticleApiInfoToPairFunction())
                .cache();

        JavaPairRDD<String, ArticleApiInfoAndArticleData> articleApiInfoAndArticleData = articleApiInfo
                .join(broadcastArticleData.value())
                .values()
                .mapToPair(new MapArticleApiInfoAndArticleDataToPairFunction()).cache();

        JavaRDD<Article> article = articleApiInfoAndArticleData
                .join(broadcastCameoEventCodes.value())
                .values()
                .map(new MapArticleApiInfoAndArticleDataAndEventCodes()).cache();

        article.saveAsTextFile(OUT_DIR);
        renameOutputFiles(OUT_DIR, hadoopConfiguration);

        fileSystem.close();
        sc.close();
    }


    private static void renameOutputFiles(String outputPath, Configuration hadoopConfiguration) throws IOException {
        FileSystem fileSystem = FileSystem.get(hadoopConfiguration);
        FileStatus[] partFileStatuses = fileSystem.globStatus(new Path(outputPath + "part*"));
        for (FileStatus fs : partFileStatuses) {
            String name = fs.getPath().getName();
            fileSystem.rename(new Path(outputPath + name), new Path(outputPath + ArticleMapping.OUTPUT_FILES_PREFIX + name));
        }
        fileSystem.close();
    }
}


