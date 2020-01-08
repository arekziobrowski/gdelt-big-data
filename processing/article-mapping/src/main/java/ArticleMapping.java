import models.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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
    private static String OUT_DIR = "/etl/staging/load/{RUN_CONTROL_DATE}/article/";
    private static final String OUTPUT_FILES_PREFIX = "article.dat-";

    private static final String RUN_CONTROL_DATE_PLACEHOLDER = "{RUN_CONTROL_DATE}";

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

        // CONFIGURATION
        SparkConf conf = new SparkConf().setAppName("RAKE Processing");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Configuration hadoopConfiguration = sc.hadoopConfiguration();
        SQLContext sqlContext = new SQLContext(sc);

        // CLEANING ARTICLE.DAT* FILES
        FileSystem fileSystem = FileSystem.get(sc.hadoopConfiguration());
        fileSystem.delete(new Path(OUT_DIR + OUTPUT_FILES_PREFIX + "*"), true);
        fileSystem.close();

        // PROCESSING
        JavaPairRDD<String, ArticleData> articleData = sc.textFile(INPUT_FILES_ARTICLES_DATA_CLEANSED_PATTERN)
                .distinct()
                .map(new MapArticleDataToArticleData())
                .filter(new ArticleDataNotNullFilter())
                .mapToPair(new MapArticleDataToPairFunction())
                .cache();

        final Broadcast<JavaPairRDD<String, ArticleData>> broadcastArticleData = sc.broadcast(articleData);


        JavaPairRDD<String, EventCode> eventCodes = sc.textFile(INPUT_FILE_CAMEO_EVENTCODES)
                .distinct()
                .map(new MapEventCodeToEventCode())
                .filter(new EventCodeNotNullFilter())
                .mapToPair(new MapEventCodeToPairFunction())
                .cache();

        final Broadcast<JavaPairRDD<String, EventCode>> broadcastCameoEventCodes = sc.broadcast(eventCodes);


        // API_INFO has the biggest amount of data, so it won't be broadcasted
        JavaPairRDD<String, ArticleApiInfo> articleApiInfo = sc.textFile(INPUT_FILE_ARTICLES_API_INFO_CLEANSED_PATTERN)
                .distinct()
                .map(new MapArticleApiInfoToArticleApiInfo())
                .filter(new ArticleApiInfoNotNullFilter())
                .mapToPair(new MapArticleApiInfoToPairFunction())
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
        renameOutputFiles(OUT_DIR, hadoopConfiguration, OUTPUT_FILES_PREFIX);

        fileSystem.close();
        sc.close();
    }

    private static class MapArticleDataToArticleData implements Function<String, ArticleData> {
        @Override
        public ArticleData call(String s) throws Exception {
            String[] spl = s.split("\t");
            ArticleData articleData = null;

            try {
                articleData = new ArticleData(
                        spl[0],
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(spl[1]),
                        new SimpleDateFormat("yyyy-MM-dd").parse(spl[2]),
                        spl[3],
                        Integer.parseInt(spl[8]),
                        spl[11]);
            } catch (Exception e) {
                return null;
            }

            return articleData;
        }
    }

    private static class MapArticleDataToPairFunction implements PairFunction<ArticleData, String, ArticleData> {
        @Override
        public Tuple2<String, ArticleData> call(ArticleData inArticleData) throws Exception {
            return new Tuple2<>(inArticleData.getSourceUrl(), inArticleData);
        }
    }

    private static class ArticleDataNotNullFilter implements Function<ArticleData, Boolean> {
        @Override
        public Boolean call(ArticleData inArticleData) throws Exception {
            return !(inArticleData == null);
        }
    }

    private static class MapEventCodeToEventCode implements Function<String, EventCode> {
        @Override
        public EventCode call(String s) throws Exception {
            String[] spl = s.split("\t", 2);
            EventCode eventCode = null;

            try {
                Integer.parseInt(spl[0]);

                eventCode = new EventCode(
                        spl[0],
                        spl[1]);
            } catch (Exception e) {
                return null;
            }

            return eventCode;
        }
    }

    private static class MapEventCodeToPairFunction implements PairFunction<EventCode, String, EventCode> {
        @Override
        public Tuple2<String, EventCode> call(EventCode inEventCode) throws Exception {
            return new Tuple2<>(inEventCode.getCameoEventCode(), inEventCode);
        }
    }

    private static class EventCodeNotNullFilter implements Function<EventCode, Boolean> {
        @Override
        public Boolean call(EventCode inEventCode) throws Exception {
            return !(inEventCode == null);
        }
    }

    private static class MapArticleApiInfoToArticleApiInfo implements Function<String, ArticleApiInfo> {
        @Override
        public ArticleApiInfo call(String s) throws Exception {
            String[] spl = s.split("\t", 7);
            ArticleApiInfo articleApiInfo = null;

            try {
                articleApiInfo = new ArticleApiInfo(
                        spl[0],
                        spl[1],
                        spl[3]);
            } catch (Exception e) {
                return null;
            }

            return articleApiInfo;
        }
    }

    private static class MapArticleApiInfoToPairFunction implements PairFunction<ArticleApiInfo, String, ArticleApiInfo> {
        @Override
        public Tuple2<String, ArticleApiInfo> call(ArticleApiInfo inArticleApiInfo) throws Exception {
            return new Tuple2<>(inArticleApiInfo.getUrl(), inArticleApiInfo);
        }
    }

    private static class ArticleApiInfoNotNullFilter implements Function<ArticleApiInfo, Boolean> {
        @Override
        public Boolean call(ArticleApiInfo articleApiInfo) throws Exception {
            return !(articleApiInfo == null);
        }
    }


    private static class MapArticleApiInfoAndArticleDataToPairFunction implements PairFunction<Tuple2<ArticleApiInfo, ArticleData>, String, ArticleApiInfoAndArticleData> {
        @Override
        public Tuple2<String, ArticleApiInfoAndArticleData> call(Tuple2<ArticleApiInfo, ArticleData> inTuple2) throws Exception {
            ArticleApiInfoAndArticleData articleApiInfoAndArticleData = new ArticleApiInfoAndArticleData(
                    inTuple2._1.getTitle(),
                    inTuple2._1.getUrl(),
                    inTuple2._2.getDateAdded(),
                    inTuple2._2.getEventDate(),
                    inTuple2._2.getEventCode(),
                    inTuple2._1.getLanguage(),
                    inTuple2._2.getAverageTone(),
                    inTuple2._2.getCountryCode());

            return new Tuple2<>(inTuple2._2.getEventCode(), articleApiInfoAndArticleData);
        }
    }

    private static class MapArticleApiInfoAndArticleDataAndEventCodes implements Function<Tuple2<ArticleApiInfoAndArticleData, EventCode>, Article> {
        @Override
        public Article call(Tuple2<ArticleApiInfoAndArticleData, EventCode> inTuple2) throws Exception {
            return new Article(
                    inTuple2._1.getTitle(),
                    inTuple2._1.getUrl(),
                    inTuple2._1.getDatePublished(),
                    inTuple2._1.getDateEvent(),
                    inTuple2._2.getEventDescription(),
                    inTuple2._1.getLanguage(),
                    inTuple2._1.getTone(),
                    inTuple2._1.getCountryId());
        }
    }

    private static void renameOutputFiles(String outputPath, Configuration hadoopConfiguration, String newPrefix) throws IOException {
        FileSystem fileSystem = FileSystem.get(hadoopConfiguration);
        FileStatus[] partFileStatuses = fileSystem.globStatus(new Path(outputPath + "part*"));
        for (FileStatus fs : partFileStatuses) {
            String name = fs.getPath().getName();
            fileSystem.rename(new Path(outputPath + name), new Path(outputPath + newPrefix + name));
        }
        fileSystem.close();
    }
}


