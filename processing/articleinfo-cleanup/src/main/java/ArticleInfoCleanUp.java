import functions.ImageDownloadMapFunction;
import functions.JsonEntryProcessedMapFunction;
import functions.RowToJsonEntryFlatMapFunction;
import functions.TextDownloadMapFunction;
import model.JsonEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;

public class ArticleInfoCleanUp {
    private static String INPUT_FILE = "/data/gdelt/{RUN_CONTROL_DATE}/api/*/article_info.json";
    private static String OUT_DIR = "/etl/staging/cleansed/{RUN_CONTROL_DATE}/api/";
    private static String OUT_DIR_ARTICLES = "/etl/staging/cleansed/{RUN_CONTROL_DATE}/texts/";
    private static String OUT_DIR_IMAGES = "/etl/staging/cleansed/{RUN_CONTROL_DATE}/images/";
    private final static String LOG_FILE = "/tech/transformation/log/articles-api-info-cleansed.log";
    private final static String OUTPUT_FILES_PREFIX = "articles-api-info-cleansed.dat-";

    public static void main(String[] args) throws IOException {
        if (args.length == 0 || !args[0].matches("[0-9]{8}")) {
            System.err.println("Provide RUN_CONTROL_DATE with args[].");
            System.exit(2);
        }
        final String RUN_CONTROL_DATE = args[0];
        INPUT_FILE = INPUT_FILE.replace("{RUN_CONTROL_DATE}", RUN_CONTROL_DATE);
        OUT_DIR = OUT_DIR.replace("{RUN_CONTROL_DATE}", RUN_CONTROL_DATE);
        OUT_DIR_ARTICLES = OUT_DIR_ARTICLES.replace("{RUN_CONTROL_DATE}", RUN_CONTROL_DATE);
        OUT_DIR_IMAGES = OUT_DIR_IMAGES.replace("{RUN_CONTROL_DATE}", RUN_CONTROL_DATE);

        SparkConf conf = new SparkConf().setAppName("article-info.json Clean-Up");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Configuration hadoopConfiguration = sc.hadoopConfiguration();

        FileSystem fileSystem = FileSystem.get(sc.hadoopConfiguration());
        if (!fileSystem.exists(new Path(LOG_FILE)))
            fileSystem.create(new Path(LOG_FILE));
        fileSystem.delete(new Path(OUT_DIR), true);
        fileSystem.delete(new Path(OUT_DIR_ARTICLES), true);
        fileSystem.delete(new Path(OUT_DIR_IMAGES), true);
        fileSystem.close();

        JavaRDD<String> jsonExampleData = sc.textFile(INPUT_FILE).cache();
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().json(jsonExampleData);
        final JavaPairRDD<JsonEntry, String> entriesWithArticles = df.toJavaRDD().flatMap(new RowToJsonEntryFlatMapFunction())
                .filter(new UrlAndSocialImageNotEmptyFilter())
                .mapToPair(new TextDownloadMapFunction(OUT_DIR_ARTICLES))
                .filter(new ArticleContentNotEmptyFilter()).cache();
        saveArticles(entriesWithArticles.collect(), hadoopConfiguration);

        final JavaPairRDD<JsonEntry, byte[]> entriesWithImages = entriesWithArticles
                .map(new Function<Tuple2<JsonEntry, String>, JsonEntry>() {
                    @Override
                    public JsonEntry call(Tuple2<JsonEntry, String> v1) throws Exception {
                        return v1._1();
                    }
                }).mapToPair(new ImageDownloadMapFunction(OUT_DIR_IMAGES, 5))
                .filter(new ImageContentNotEmptyFilter()).cache();
        saveImages(entriesWithImages.collect(), hadoopConfiguration);

        entriesWithImages
                .map(new Function<Tuple2<JsonEntry, byte[]>, JsonEntry>() {
                    @Override
                    public JsonEntry call(Tuple2<JsonEntry, byte[]> v1) throws Exception {
                        return v1._1();
                    }
                }).map(new JsonEntryProcessedMapFunction()).saveAsTextFile(OUT_DIR);

        renameOutputFiles(OUT_DIR, hadoopConfiguration, OUTPUT_FILES_PREFIX);
    }

    private static void saveArticles(List<Tuple2<JsonEntry, String>> articles, Configuration hadoopConfiguration) throws IOException {
        FileSystem fileSystem = FileSystem.get(hadoopConfiguration);
        for (Tuple2<JsonEntry, String> a : articles) {
            log("Saving new article to: " + a._1().getArticlePath(), fileSystem);
            FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(a._1().getArticlePath()));
            BufferedOutputStream os = new BufferedOutputStream(fsDataOutputStream);
            os.write(a._2().getBytes(StandardCharsets.UTF_8));
            os.close();
            fsDataOutputStream.close();
        }
        fileSystem.close();
    }

    private static void saveImages(List<Tuple2<JsonEntry, byte[]>> images, Configuration hadoopConfiguration) throws IOException {
        FileSystem fileSystem = FileSystem.get(hadoopConfiguration);
        for (Tuple2<JsonEntry, byte[]> i : images) {
            log("Saving new image to: " + i._1().getImagePath(), fileSystem);
            FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(i._1().getImagePath()));
            BufferedOutputStream os = new BufferedOutputStream(fsDataOutputStream);
            os.write(i._2());
            os.close();
            fsDataOutputStream.close();
        }
        fileSystem.close();

    }

    private static void renameOutputFiles(String outputPath, Configuration hadoopConfiguration, String newPrefix) throws IOException {
        FileSystem fileSystem = FileSystem.get(hadoopConfiguration);
        log("Renaming files to common prefix: " + newPrefix,fileSystem);
        FileStatus[] partFileStatuses = fileSystem.globStatus(new Path(outputPath + "part*"));
        for (FileStatus fs : partFileStatuses) {
            String name = fs.getPath().getName();
            fileSystem.rename(new Path(outputPath + name), new Path(outputPath + newPrefix + name));
        }
        log("Done renaming", fileSystem);
        fileSystem.close();
    }

    private static void log(String logMessage, FileSystem fileSystem) throws IOException {
        String logLine = "ArticleInfoCleanUp | " + new Date().toString() + " | " + logMessage + '\n';
        System.out.print(logLine);
        FSDataOutputStream fsAppendStream = fileSystem.append(new Path(LOG_FILE));
        BufferedOutputStream appendStream = new BufferedOutputStream(fsAppendStream);
        appendStream.write(logLine.getBytes(StandardCharsets.UTF_8));
        appendStream.close();
        fsAppendStream.close();
    }

    static class UrlAndSocialImageNotEmptyFilter implements Function<JsonEntry, Boolean> {
        @Override
        public Boolean call(JsonEntry v1) throws Exception {
            return !(v1.getUrl() == null || v1.getUrl().isEmpty() || v1.getSocialImage() == null || v1.getSocialImage().isEmpty());
        }
    }

    static class ArticleContentNotEmptyFilter implements Function<Tuple2<JsonEntry, String>, Boolean> {
        @Override
        public Boolean call(Tuple2<JsonEntry, String> v1) throws Exception {
            return !v1._2().isEmpty();
        }
    }

    static class ImageContentNotEmptyFilter implements Function<Tuple2<JsonEntry, byte[]>, Boolean> {
        @Override
        public Boolean call(Tuple2<JsonEntry, byte[]> v1) throws Exception {
            return v1._2() != null && v1._2().length != 0;
        }
    }
}