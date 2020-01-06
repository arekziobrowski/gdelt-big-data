import functions.FlatMapToOutputRowFunction;
import functions.RakePairMapper;
import model.OutputRow;
import model.Phrase;
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
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public class RakeKeyWordsProcessing {
    private final static String LOG_FILE = "/home/jakub/2sem/rake/logi";
    private static String LOOKUP_FILE = "/home/jakub/2sem/rake/lookup";
    private static String INPUT_PATH = "/home/jakub/2sem/rake/articles-api-info-cleansed.dat-part-*";
    private static String ARTICLES_PATH = "/home/jakub/2sem/article-info/texts/";
    private final static String STOP_WORDS = "/home/jakub/2sem/rake/stop-words";
    private static String OUT_DIR = "/home/jakub/2sem/rake/output/";
    private final static String OUTPUT_FILES_PREFIX = "keyword_metadata.dat";

    public static void main(String[] args) throws IOException {
        //CONSTANTS
        if (args.length == 0 || !args[0].matches("[0-9]{8}")) {
            System.err.println("Provide RUN_CONTROL_DATE with args[].");
            System.exit(2);
        }
        final String RUN_CONTROL_DATE = args[0];
        INPUT_PATH = INPUT_PATH.replace("{RUN_CONTROL_DATE}", RUN_CONTROL_DATE);
        LOOKUP_FILE = LOOKUP_FILE.replace("{RUN_CONTROL_DATE}", RUN_CONTROL_DATE);
        ARTICLES_PATH = ARTICLES_PATH.replace("{RUN_CONTROL_DATE}", RUN_CONTROL_DATE);
        OUT_DIR = OUT_DIR.replace("{RUN_CONTROL_DATE}", RUN_CONTROL_DATE);
        //CONFIGURATION
        SparkConf conf = new SparkConf().setMaster("local").setAppName("RAKE Processing");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Configuration hadoopConfiguration = sc.hadoopConfiguration();
        SQLContext sqlContext = new SQLContext(sc);
        //CLEANING DIRS
        FileSystem fileSystem = FileSystem.get(sc.hadoopConfiguration());
        if (!fileSystem.exists(new Path(LOG_FILE)))
            fileSystem.create(new Path(LOG_FILE));
        fileSystem.delete(new Path(OUT_DIR), true);
        fileSystem.close();
        //PROCESSING
        log("RAKE Processing starting...", sc.hadoopConfiguration());
        final List<String> stopWords = sc.textFile(STOP_WORDS).collect();
        log("Loaded stop words for RAKE processing from: " + STOP_WORDS, sc.hadoopConfiguration());

        final JavaPairRDD<String, String> articleIdAndPathPairs = sc.textFile(INPUT_PATH)
                .mapToPair(new MapArticleInfoToPairFunction())
                .join(sc.textFile(LOOKUP_FILE).mapToPair(new MapToLookupPairFunction()))
                .values()
                .mapToPair(new Identity())
                .filter(new KeyValueNotEmptyFilter());

        final JavaPairRDD<String, Phrase[]> pathRAKEKeywords = sc.wholeTextFiles(ARTICLES_PATH)
                .mapToPair(new RakePairMapper(stopWords));

        final JavaRDD<OutputRow> outputRowJavaRDD = pathRAKEKeywords.join(articleIdAndPathPairs).values()
                .flatMap(new FlatMapToOutputRowFunction());

        outputRowJavaRDD.saveAsTextFile(OUT_DIR);
        renameOutputFiles(OUT_DIR, hadoopConfiguration, OUTPUT_FILES_PREFIX);
    }

    private static void renameOutputFiles(String outputPath, Configuration hadoopConfiguration, String newPrefix) throws IOException {
        FileSystem fileSystem = FileSystem.get(hadoopConfiguration);
        log("Renaming files to common prefix: " + newPrefix, fileSystem);
        FileStatus[] partFileStatuses = fileSystem.globStatus(new Path(outputPath + "part*"));
        for (FileStatus fs : partFileStatuses) {
            String name = fs.getPath().getName();
            fileSystem.rename(new Path(outputPath + name), new Path(outputPath + newPrefix + name));
        }
        log("Done renaming", fileSystem);
        fileSystem.close();
    }

    private static void log(String logMessage, Configuration hadoopConfiguration) throws IOException {
        FileSystem fileSystem = FileSystem.get(hadoopConfiguration);
        String logLine = "ArticleInfoCleanUp | " + new Date().toString() + " | " + logMessage + '\n';
        System.out.print(logLine);
//        FSDataOutputStream fsAppendStream = fileSystem.append(new Path(LOG_FILE));
//        BufferedOutputStream appendStream = new BufferedOutputStream(fsAppendStream);
//        appendStream.write(logLine.getBytes(StandardCharsets.UTF_8));
//        appendStream.close();
//        fsAppendStream.close();
//        fileSystem.close();
    }

    private static void log(String logMessage, FileSystem fileSystem) throws IOException {
        String logLine = "ArticleInfoCleanUp | " + new Date().toString() + " | " + logMessage + '\n';
        System.out.print(logLine);
//        FSDataOutputStream fsAppendStream = fileSystem.append(new Path(LOG_FILE));
//        BufferedOutputStream appendStream = new BufferedOutputStream(fsAppendStream);
//        appendStream.write(logLine.getBytes(StandardCharsets.UTF_8));
//        appendStream.close();
//        fsAppendStream.close();
    }

    static class Identity implements PairFunction<Tuple2<String, String>, String, String> {
        @Override
        public Tuple2<String, String> call(Tuple2<String, String> input) throws Exception {
            return input;
        }
    }

    static class MapArticleInfoToPairFunction implements PairFunction<String, String, String> {
        @Override
        public Tuple2<String, String> call(String s) throws Exception {
            String[] splitted = s.split("\t", 7);
            return new Tuple2<>(splitted[0], splitted.length == 7 ? splitted[6] : "");
        }
    }

    static class MapToLookupPairFunction implements PairFunction<String, String, String> {
        @Override
        public Tuple2<String, String> call(String s) throws Exception {
            String[] splitted = s.split("\t", 2);
            return new Tuple2<>(splitted[1], splitted[0]);
        }
    }

    static class KeyValueNotEmptyFilter implements Function<Tuple2<String, String>, Boolean> {
        @Override
        public Boolean call(Tuple2<String, String> v1) throws Exception {
            return !(v1._1().isEmpty() || v1._2().isEmpty());
        }
    }
}
