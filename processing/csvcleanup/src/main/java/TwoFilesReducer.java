import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

public class TwoFilesReducer extends Reducer<Text, Text, NullWritable, Text> {
    private MultipleOutputs<NullWritable, Text> mos;
    private static final String OUTPUT_NAME = "out";
    private String cameoFileName;
    private String dataFile;
    private String rejectFile;
    private BufferedReader bufferedReader;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<>(context);

        Configuration conf = context.getConfiguration();
        dataFile = conf.getStrings("data_file")[0];
        rejectFile = conf.getStrings("reject_file")[0];
        URI[] cameoURIs = Job.getInstance(context.getConfiguration())
                .getCacheFiles();
        Path cameoPath = new Path(cameoURIs[0].getPath());
        cameoFileName = cameoPath.getName();

    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String outFile = validateCountryCode(key.toString()) ? dataFile : rejectFile;
        context.setStatus("Reducing " + key.toString() + " key...");
        System.out.println("Reducing " + key.toString() + " key...");
        for (Text value : values)
            mos.write(OUTPUT_NAME, NullWritable.get(), value, outFile);
    }

    private boolean validateCountryCode(String countryCode) throws IOException {
        if (countryCode.isEmpty())
            return false;
        bufferedReader = new BufferedReader(new FileReader(this.cameoFileName));
//        CAMEO FILE HAS HEADER  ROW "CODE\tLABEL" BEFORE IT'S CONTENT!
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            if (countryCode.equals(line.split("\t")[0]))
                return true;
        }

        return false;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }


}
