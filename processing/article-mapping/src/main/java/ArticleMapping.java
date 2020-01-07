import models.ArticleApiInfo;
import models.ArticleData;
import models.EventCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class ArticleMapping {
    private static String INPUT_FILES_ARTICLES_DATA_CLEANSED_PATTERN = "/etl/staging/cleansed/{RUN_CONTROL_DATE}/articles-data-cleansed.dat-*";
    private static String INPUT_FILE_ARTICLES_API_INFO_CLEANSED_PATTERN = "/etl/staging/cleansed/{RUN_CONTROL_DATE}/api/articles-api-info-cleansed.dat-*";
    private static String INPUT_FILE_CAMEO_EVENTCODES = "/data/gdelt/{RUN_CONTROL_DATE}/cameo/CAMEO.eventcodes.txt";
    private static String OUT_DIR = "/etl/staging/load/{RUN_CONTROL_DATE}/";
    private static final String OUTPUT_FILES_PREFIX = "article.dat-";

    private static final String RUN_CONTROL_DATE_PLACEHOLDER = "{RUN_CONTROL_DATE}";

//    private static final String DELIMITER = "\t";


    public static void main(String[] args) throws Exception {
        if (args.length == 0 || !args[0].matches("[0-9]{8}")) {
            System.err.println("Provide RUN_CONTROL_DATE with args[].");
            System.exit(2);
        }
        final String RUN_CONTROL_DATE = args[0];

        //CONSTANTS
        INPUT_FILES_ARTICLES_DATA_CLEANSED_PATTERN = INPUT_FILES_ARTICLES_DATA_CLEANSED_PATTERN.replace(RUN_CONTROL_DATE_PLACEHOLDER, RUN_CONTROL_DATE);
        INPUT_FILE_ARTICLES_API_INFO_CLEANSED_PATTERN = INPUT_FILE_ARTICLES_API_INFO_CLEANSED_PATTERN.replace(RUN_CONTROL_DATE_PLACEHOLDER, RUN_CONTROL_DATE);
        INPUT_FILE_CAMEO_EVENTCODES = INPUT_FILE_CAMEO_EVENTCODES.replace(RUN_CONTROL_DATE_PLACEHOLDER, RUN_CONTROL_DATE);
        OUT_DIR = OUT_DIR.replace(RUN_CONTROL_DATE_PLACEHOLDER, RUN_CONTROL_DATE);

        //CONFIGURATION
        SparkConf conf = new SparkConf().setAppName("RAKE Processing");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Configuration hadoopConfiguration = sc.hadoopConfiguration();
        SQLContext sqlContext = new SQLContext(sc);

        //CLEANING ARTICLE.DAT-* FILES
        FileSystem fileSystem = FileSystem.get(sc.hadoopConfiguration());
        fileSystem.delete(new Path(OUT_DIR + OUTPUT_FILES_PREFIX + "*"), true);
        fileSystem.close();

        //PROCESSING
        JavaPairRDD<String, ArticleData> articleData = sc.textFile(INPUT_FILES_ARTICLES_DATA_CLEANSED_PATTERN)
                .mapToPair(new MapArticleDataToPairFunction());

        // todo: cut header in bash
        JavaPairRDD<String, EventCode> eventCodes = sc.textFile(INPUT_FILE_CAMEO_EVENTCODES)
                .mapToPair(new MapEventCodeToPairFunction());

        final Broadcast<JavaPairRDD<String, ArticleData>> broadcastArticleData = sc.broadcast(articleData);
        final Broadcast<JavaPairRDD<String, EventCode>> broadcastCameoEventCodes = sc.broadcast(eventCodes);


        //API_INFO has the biggest amount of data
        JavaPairRDD<String, ArticleApiInfo> articleApiInfo = sc.textFile(INPUT_FILE_ARTICLES_API_INFO_CLEANSED_PATTERN)
                .mapToPair(new MapArticleApiInfoToPairFunction());

        //        System.out.println(eventCodes.collect());
        // todo: poprawic blad w cleansed
//        System.out.println(articleData.collect());
//        System.out.println(articleApiInfo.collect());


//        JavaPairRDD<String, Tuple2<String[], String[]>> articleApiInfoWithArticleData = articleApiInfo.join(broadcastArticleData.value());
//        JavaRDD<String[]> outputRow = articleApiInfoWithArticleData.flatMap()
//        JavaPairRDD<String, >articleApiInfoWithArticleData


        fileSystem.close();
        sc.close();
    }

    static class MapArticleDataToPairFunction implements PairFunction<String, String, ArticleData> {
        @Override
        public Tuple2<String, ArticleData> call(String s) throws Exception {
            String[] spl = s.split("\t");
            ArticleData articleData = new ArticleData(
                    spl[0],
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(spl[1]),
                    new SimpleDateFormat("yyyy-MM-dd").parse(spl[2]),
                    Integer.parseInt(spl[8]),
                    spl[11]);
            return new Tuple2<>(spl[0], articleData);
        }
    }

    static class MapEventCodeToPairFunction implements PairFunction<String, String, EventCode> {
        @Override
        public Tuple2<String, EventCode> call(String s) throws Exception {
            String[] spl = s.split("\t", 2);
            EventCode eventCode = new EventCode(
                    spl[0],
                    spl.length == 2 ? spl[1] : "");
            return new Tuple2<>(spl[0], eventCode);
        }
    }

    static class MapArticleApiInfoToPairFunction implements PairFunction<String, String, ArticleApiInfo> {
        @Override
        public Tuple2<String, ArticleApiInfo> call(String s) throws Exception {
            String[] spl = s.split("\t", 7);
            ArticleApiInfo articleApiInfo = new ArticleApiInfo(
                    spl[0],
                    spl[1],
                    spl[3]);
            return new Tuple2<>(spl[0], articleApiInfo);
        }
    }
}

//    private static void renameOutputFiles(String outputPath, Configuration hadoopConfiguration, String newPrefix) throws IOException {
//        FileSystem fileSystem = FileSystem.get(hadoopConfiguration);
//        FileStatus[] partFileStatuses = fileSystem.globStatus(new Path(outputPath + "part*"));
//        for (FileStatus fs : partFileStatuses) {
//            String name = fs.getPath().getName();
//            fileSystem.rename(new Path(outputPath + name), new Path(outputPath + newPrefix + name));
//        }
//        fileSystem.close();
//    }
//}
