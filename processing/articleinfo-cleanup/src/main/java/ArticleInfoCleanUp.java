import de.l3s.boilerpipe.BoilerpipeProcessingException;
import functions.ImageDownloadMapFunction;
import functions.JsonEntryProcessedMapFunction;
import functions.RowToJsonEntryFlatMapFunction;
import functions.TextDownloadMapFunction;
import model.JsonEntry;
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
import java.util.List;

public class ArticleInfoCleanUp {
    private final static String INPUT_FILE = "/home/jakub/2sem/big_data/gdelt-big-data/processing/articleinfo-cleanup/example.json";
    private final static String OUT_DIR = "/home/jakub/2sem/big-data/out_temp_1/csv/";
    private final static String OUT_DIR_ARTICLES = "/home/jakub/2sem/big-data/out_temp_1/articles/";
    private final static String OUT_DIR_IMAGES = "/home/jakub/2sem/big-data/out_temp_1/images/";
    private final static String OUTPUT_FILES_PREFIX = "articles-api-info-cleansed.dat-";


    public static void main(String[] args) throws BoilerpipeProcessingException, IOException {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("article-info.json Clean-Up");
        JavaSparkContext sc = new JavaSparkContext(conf);
        FileSystem fileSystem = FileSystem.get(sc.hadoopConfiguration());

        fileSystem.delete(new Path(OUT_DIR), true);
        fileSystem.delete(new Path(OUT_DIR_ARTICLES), true);
        fileSystem.delete(new Path(OUT_DIR_IMAGES), true);

        JavaRDD<String> jsonExampleData = sc.textFile(INPUT_FILE).cache();
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().json(jsonExampleData);
        final JavaPairRDD<JsonEntry, String> entriesWithArticles = df.toJavaRDD().flatMap(new RowToJsonEntryFlatMapFunction())
                .filter(new UrlAndSocialImageNotEmptyFilter())
                .mapToPair(new TextDownloadMapFunction(OUT_DIR_ARTICLES))
                .filter(new ArticleContentNotEmptyFilter()).cache();
        saveArticles(entriesWithArticles.collect(), fileSystem);

        final JavaPairRDD<JsonEntry, byte[]> entriesWithImages = entriesWithArticles
                .map(new Function<Tuple2<JsonEntry, String>, JsonEntry>() {
                    @Override
                    public JsonEntry call(Tuple2<JsonEntry, String> v1) throws Exception {
                        return v1._1();
                    }
                }).mapToPair(new ImageDownloadMapFunction(OUT_DIR_IMAGES, 5))
                .filter(new ImageContentNotEmptyFilter()).cache();
        saveImages(entriesWithImages.collect(), fileSystem);

        entriesWithImages
                .map(new Function<Tuple2<JsonEntry, byte[]>, JsonEntry>() {
                    @Override
                    public JsonEntry call(Tuple2<JsonEntry, byte[]> v1) throws Exception {
                        return v1._1();
                    }
                }).map(new JsonEntryProcessedMapFunction()).saveAsTextFile(OUT_DIR);

        renameOutputFiles(OUT_DIR, fileSystem, OUTPUT_FILES_PREFIX);
    }

    private static void saveArticles(List<Tuple2<JsonEntry, String>> articles, FileSystem fileSystem) throws IOException {
        for (Tuple2<JsonEntry, String> a : articles) {
            FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(a._1().getArticlePath()));
            BufferedOutputStream os = new BufferedOutputStream(fsDataOutputStream);
            os.write(a._2().getBytes(StandardCharsets.UTF_8));
            os.close();
        }
    }

    private static void saveImages(List<Tuple2<JsonEntry, byte[]>> images, FileSystem fileSystem) throws IOException {
        for (Tuple2<JsonEntry, byte[]> i : images) {
            FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(i._1().getImagePath()));
            BufferedOutputStream os = new BufferedOutputStream(fsDataOutputStream);
            os.write(i._2());
            os.close();
        }
    }

    private static void renameOutputFiles(String outputPath, FileSystem fileSystem, String newPrefix) throws IOException {
        FileStatus[] partFileStatuses = fileSystem.globStatus(new Path(outputPath + "part*"));
        for (FileStatus fs : partFileStatuses) {
            String name = fs.getPath().getName();
            fileSystem.rename(new Path(outputPath + name), new Path(outputPath + newPrefix + name));
        }
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