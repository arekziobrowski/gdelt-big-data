import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class CsvDistinct {
    private static final String IN_PATH = "/data/gdelt/{RUN_CONTROL_DATE}/csv/";
    private static final String OUT_PATH = "/etl/staging/cleansed/{RUN_CONTROL_DATE}/distinct/";
    private static final String JOB_NAME = "csv-distinct";

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
        job.setJarByClass(CsvDistinct.class);
        job.setMapperClass(UrlMapper.class);
        job.setReducerClass(DistinctReducer.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(IN_PATH.replace("{RUN_CONTROL_DATE}", RUN_CONTROL_DATE)));
        FileOutputFormat.setOutputPath(job, new Path(OUT_PATH.replace("{RUN_CONTROL_DATE}", RUN_CONTROL_DATE)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    static class UrlMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final int CSV_COLUMN_NUMBER = 61;
        private static final int URL_COLUMN_NUMBER = 60;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitted = value.toString().split("\t", CSV_COLUMN_NUMBER);
            if (splitted.length == CSV_COLUMN_NUMBER && !splitted[URL_COLUMN_NUMBER].isEmpty())
                context.write(new Text(splitted[URL_COLUMN_NUMBER]), value);
        }
    }

    static class DistinctReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            final Text firstUrlOccurrence = values.iterator().next();
            context.write(NullWritable.get(), firstUrlOccurrence);
        }
    }
}