import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class CsvMapperTest {
    @Mock
    private Mapper<LongWritable, Text, Text, Text>.Context mockContext;

    private CsvMapper csvMapper;

    @Before
    public void setUp() throws Exception {
        this.csvMapper = new CsvMapper();
    }

    private void captureAndAssertKeyValue(Mapper<LongWritable, Text, Text, Text>.Context mock, String key, String value) throws IOException, InterruptedException {
        ArgumentCaptor<Text> keyCaptor = ArgumentCaptor.forClass(Text.class);
        ArgumentCaptor<Text> valueCaptor = ArgumentCaptor.forClass(Text.class);
        Mockito.verify(mock, Mockito.times(1)).write(keyCaptor.capture(), valueCaptor.capture());
        Assert.assertEquals(key, keyCaptor.getValue().toString());
        Assert.assertEquals(value, valueCaptor.getValue().toString());
    }

    @Test
    public void invalidCSVFormatTest() throws IOException, InterruptedException {
        final String line = "881990832\t20181024\t201810\t2018\t2018.8055\t\t\t\t\t\t\t\t\tAUS" + "" +
                "\tVICTORIA\tAUS\t\t\t\t\t\t\t\t0\t139\t139\t13\t3\t-7.0\t3\t1\t3\t2.02774813233724\t0\t\t\t\t\t\t" +
                "\t\t4\tVancouver, British Columbia, Canada\tCA\tCA02\t12552\t49.25\t-123.133\t-575268\t4\tVancouver," +
                " British Columbia, Canada\tCA\tCA02\t12552\t49.25\t-123.133\t-575268\t20191024143000\thttps://www.ti" +
                "mescolonist.com/entertainment/jeremy-dutcher-brings-ancestors-songs-to-life-1.23985964";
        csvMapper.map(new LongWritable(1), new Text(line), mockContext);
        captureAndAssertKeyValue(this.mockContext, "", line);
    }

    @Test
    public void missingCountryCodeTest() throws IOException, InterruptedException {
        final String line = "881990832\t20181024\t201810\t2018\t2018.8055\t\t\t\t\t\t\t\t\t\t\tAUS" + "" +
                "\tVICTORIA\tAUS\t\t\t\t\t\t\t\t0\t139\t139\t13\t3\t-7.0\t3\t1\t3\t2.02774813233724\t0\t\t\t\t\t\t" +
                "\t\t4\tVancouver, British Columbia, Canada\tCA\tCA02\t12552\t49.25\t-123.133\t-575268\t4\tVancouver," +
                " British Columbia, Canada\t\tCA02\t12552\t49.25\t-123.133\t-575268\t20191024143000\thttps://www.ti" +
                "mescolonist.com/entertainment/jeremy-dutcher-brings-ancestors-songs-to-life-1.23985964";
        csvMapper.map(new LongWritable(1), new Text(line), mockContext);
        captureAndAssertKeyValue(this.mockContext, "", line);
    }

    @Test
    public void invalidDateLengthTest() throws IOException, InterruptedException {
        final String line = "881990832\t20181024\t201810\t2018\t2018.8055\t\t\t\t\t\t\t\t\t\t\tAUS" + "" +
                "\tVICTORIA\tAUS\t\t\t\t\t\t\t\t0\t139\t139\t13\t3\t-7.0\t3\t1\t3\t2.02774813233724\t0\t\t\t\t\t\t" +
                "\t\t4\tVancouver, British Columbia, Canada\tCA\tCA02\t12552\t49.25\t-123.133\t-575268\t4\tVancouver," +
                " British Columbia, Canada\tCA\tCA02\t12552\t49.25\t-123.133\t-575268\t2019102414000\thttps://www.ti" +
                "mescolonist.com/entertainment/jeremy-dutcher-brings-ancestors-songs-to-life-1.23985964";
        csvMapper.map(new LongWritable(1), new Text(line), mockContext);
        captureAndAssertKeyValue(this.mockContext, "", line);
    }

    @Test
    public void invalidEventDateLengthTest() throws IOException, InterruptedException {
        final String line = "881990832\t201810024\t201810\t2018\t2018.8055\t\t\t\t\t\t\t\t\t\t\tAUS" + "" +
                "\tVICTORIA\tAUS\t\t\t\t\t\t\t\t0\t139\t139\t13\t3\t-7.0\t3\t1\t3\t2.02774813233724\t0\t\t\t\t\t\t" +
                "\t\t4\tVancouver, British Columbia, Canada\tCA\tCA02\t12552\t49.25\t-123.133\t-575268\t4\tVancouver," +
                " British Columbia, Canada\tCA\tCA02\t12552\t49.25\t-123.133\t-575268\t20191024143000\thttps://www.ti" +
                "mescolonist.com/entertainment/jeremy-dutcher-brings-ancestors-songs-to-life-1.23985964";
        csvMapper.map(new LongWritable(1), new Text(line), mockContext);
        captureAndAssertKeyValue(this.mockContext, "", line);
    }

    @Test
    public void dateAndDatetimeHandlingTest() throws IOException, InterruptedException {
        final String line = "881990832\t20181024\t201810\t2018\t2018.8055\t\t\t\t\t\t\t\t\t\t\tAUS" + "" +
                "\tVICTORIA\tAUS\t\t\t\t\t\t\t\t0\t139\t139\t13\t3\t-7.0\t3\t1\t3\t2.02774813233724\t0\t\t\t\t\t\t" +
                "\t\t4\tVancouver, British Columbia, Canada\tCA\tCA02\t12552\t49.25\t-123.133\t-575268\t4\tVancouver," +
                " British Columbia, Canada \tCA\tCA02\t12552\t49.25\t-123.133\t-575268\t20191024143000\thttps://www.ti" +
                "mescolonist.com/entertainment/jeremy-dutcher-brings-ancestors-songs-to-life-1.23985964";
        final String expectedOutput = "https://www.timescolonist.com/entertainment/jeremy-dutcher-brings-ancestors-songs-"+
                "to-life-1.23985964\t2019-10-24 14:30:00\t2018-10-24\t139\t-7.0\t3\t1\t3\t2.02774813233724\t4\tVancouver, British"+
                " Columbia, Canada\tCA";
        csvMapper.map(new LongWritable(1), new Text(line), mockContext);
        captureAndAssertKeyValue(this.mockContext, "CA", expectedOutput);
    }

    @Test
    public void trimValuesTest() throws IOException, InterruptedException {
        final String line = "881990832  \t20181024\t201810\t2018\t2018.8055\t\t\t\t\t\t\t\t\t\t\tAUS" + "" +
                "\tVICTORIA\tAUS\t\t\t\t\t\t\t\t0\t139\t139\t13\t3\t-7.0\t3\t1\t3\t2.02774813233724\t0\t\t\t\t\t\t" +
                "\t\t4\tVancouver, British Columbia, Canada\tCA\tCA02\t12552\t49.25\t-123.133\t-575268\t4\tVancouver," +
                " British Columbia, Canada \t  CA  \tCA02\t12552\t49.25\t-123.133\t-575268\t20191024143000\thttps://www.ti" +
                "mescolonist.com/entertainment/jeremy-dutcher-brings-ancestors-songs-to-life-1.23985964";
        final String expectedOutput = "https://www.timescolonist.com/entertainment/jeremy-dutcher-brings-ancestors-songs-"+
                "to-life-1.23985964\t2019-10-24 14:30:00\t2018-10-24\t139\t-7.0\t3\t1\t3\t2.02774813233724\t4\tVancouver, British"+
                " Columbia, Canada\tCA";
        csvMapper.map(new LongWritable(1), new Text(line), mockContext);
        captureAndAssertKeyValue(this.mockContext, "CA", expectedOutput);
    }

    @Test
    public void missingRequiredColumnTest() throws IOException, InterruptedException {
        final String line = "881990832\t20181024\t201810\t2018\t2018.8055\t\t\t\t\t\t\t\t\t\t\tAUS" + "" +
                "\tVICTORIA\tAUS\t\t\t\t\t\t\t\t0\t139\t139\t13\t3\t\t3\t1\t3\t2.02774813233724\t0\t\t\t\t\t\t" +
                "\t\t4\tVancouver, British Columbia, Canada\tCA\tCA02\t12552\t49.25\t-123.133\t-575268\t4\tVancouver," +
                " British Columbia, Canada \tCA\tCA02\t12552\t49.25\t-123.133\t-575268\t20191024143000\thttps://www.ti" +
                "mescolonist.com/entertainment/jeremy-dutcher-brings-ancestors-songs-to-life-1.23985964";
        csvMapper.map(new LongWritable(1), new Text(line), mockContext);
        captureAndAssertKeyValue(this.mockContext, "", line);
    }
}
