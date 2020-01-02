import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;

public class CsvCleanUp {
    //    private static final String IN_PATH = "/home/jakub/2sem/big_data/20191024143000.export.CSV";
//    private static final String OUT_PATH = "/home/jakub/2sem/big_data/out";
    private static final String IN_PATH = "/data/gdelt/{RUN_CONTROL_DATE}/csv/";
    private static final String OUT_PATH = "/etl/staging/cleansed/{RUN_CONTROL_DATE}/";
    private static final String CAMEO_PATH = "/data/gdelt/{RUN_CONTROL_DATE}/cameo/CAMEO.country.txt";
    private static final String JOB_NAME = "csv-clean-up";

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || !args[0].matches("[0-9]{8}")) {
            System.err.println("Provide RUN_CONTROL_DATE with args[].");
            System.exit(2);
        }
        final String RUN_CONTROL_DATE = args[0];
        Configuration conf = new Configuration();
        Path outputPath = new Path(OUT_PATH.replace("{RUN_CONTROL_DATE}", RUN_CONTROL_DATE));
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        //It will delete the output directory if it already exists. don't need to delete it  manually
        fs.delete(outputPath, true);

        Job job = Job.getInstance(conf, JOB_NAME);
        job.setJarByClass(CsvCleanUp.class);
        job.setMapperClass(CsvMapper.class);
        job.setReducerClass(TwoFilesReducer.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
//        Cached CAMEO file and output files
        job.addCacheFile(new Path(CAMEO_PATH.replace("{RUN_CONTROL_DATE}", RUN_CONTROL_DATE)).toUri());
        job.getConfiguration().setStrings("data_file", "articles-data-cleansed.dat");
        job.getConfiguration().setStrings("reject_file", "articles-data-cleansed.reject");

        FileInputFormat.setInputPaths(job, new Path(IN_PATH.replace("{RUN_CONTROL_DATE}", RUN_CONTROL_DATE)));
        FileOutputFormat.setOutputPath(job, new Path(OUT_PATH.replace("{RUN_CONTROL_DATE}", RUN_CONTROL_DATE)));
        MultipleOutputs.addNamedOutput(job, "out", TextOutputFormat.class, NullWritable.class, Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}