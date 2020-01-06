import model.CameoCountry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;

public class CountryMapper {
    private static String INPUT_FILE = "/data/gdelt/{RUN_CONTROL_DATE}/cameo/CAMEO.country.txt";
    private static String OUT_DIR = "/etl/staging/load/{RUN_CONTROL_DATE}/";
    private final static String OUTPUT_FILES_PREFIX = "country.dat-";

    private static final String DELIMITER = "\t";

    public static void main(String[] args) throws IOException {
        if (args.length == 0 || !args[0].matches("[0-9]{8}")) {
            System.err.println("Provide RUN_CONTROL_DATE with args[].");
            System.exit(2);
        }

        final String RUN_CONTROL_DATE = args[0];
        INPUT_FILE = INPUT_FILE.replace("{RUN_CONTROL_DATE}", RUN_CONTROL_DATE);
        OUT_DIR = OUT_DIR.replace("{RUN_CONTROL_DATE}", RUN_CONTROL_DATE);

        SparkConf sparkConf = new SparkConf().setAppName("CAMEO.country.txt Mapping");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Configuration hadoopConfiguration = sc.hadoopConfiguration();

        FileSystem fileSystem = FileSystem.get(hadoopConfiguration);
        fileSystem.delete(new Path(OUT_DIR), true);
        fileSystem.close();

        sc.textFile(INPUT_FILE).map(new Function<String, CameoCountry>() {
            @Override
            public CameoCountry call(String s) throws Exception {
                return stringToCameoCountry(s);
            }
        }).saveAsTextFile(OUT_DIR);

        renameOutputFiles(OUT_DIR, hadoopConfiguration, OUTPUT_FILES_PREFIX);
        fileSystem.close();
        sc.close();
    }

    private static CameoCountry stringToCameoCountry(String s) {
        String[] spl = s.split(DELIMITER);
        return new CameoCountry(
                spl[0],
                spl[1]
        );
    }

    private static void renameOutputFiles(String outputPath, Configuration hadoopConfiguration, String newPrefix)
            throws IOException {
        FileSystem fileSystem = FileSystem.get(hadoopConfiguration);
        FileStatus[] partFileStatuses = fileSystem.globStatus(new Path(outputPath + "part*"));
        for (FileStatus fs : partFileStatuses) {
            String name = fs.getPath().getName();
            fileSystem.rename(new Path(outputPath + name), new Path(outputPath + newPrefix + name));
        }
        fileSystem.close();
    }
}
