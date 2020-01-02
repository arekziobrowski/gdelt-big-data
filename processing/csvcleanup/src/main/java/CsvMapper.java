import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CsvMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final int[] REQUIRED_COLUMNS_NUMBERS = new int[]{60, 59, 1, 26, 30, 31, 32, 33, 34, 51, 52, 53};
    private static final int CSV_COLUMN_NUMBER = 61;
    private static final int COUNTRY_CODE_COLUMN_NUMBER = 53;
    private static final int DAY_COLUMN_NUMBER = 1;
    private static final int DATEADDED_COLUMN_NUMBER = 59;
    private int counter = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.setStatus("Mapper counter: " + Integer.toString(counter));
        final String[] lineSplitted = value.toString().split("\t");
        counter++;

        boolean invalid = false;
        if (lineSplitted.length != CSV_COLUMN_NUMBER || !validate(lineSplitted))
            invalid = true;
        else {
            lineSplitted[DAY_COLUMN_NUMBER] = handleDate(lineSplitted[DAY_COLUMN_NUMBER]);
            lineSplitted[DATEADDED_COLUMN_NUMBER] = handleDatetime(lineSplitted[DATEADDED_COLUMN_NUMBER]);
            if (lineSplitted[DAY_COLUMN_NUMBER] == null || lineSplitted[DATEADDED_COLUMN_NUMBER] == null)
                invalid = true;
        }
        if (invalid) {
            context.write(new Text(""), value);
            return;
        }
        context.write(new Text(StringUtils.trim(lineSplitted[COUNTRY_CODE_COLUMN_NUMBER])),
                new Text(mapGdeltLine(lineSplitted)));
    }

    private String handleDate(String date) {
        String trimmed = StringUtils.trim(date);
        if (!trimmed.matches("[0-9]{8}"))
            return null;
        return trimmed.substring(0, 4) + "-" + trimmed.substring(4, 6) + "-" + trimmed.substring(6);
    }

    private String handleDatetime(String dateTime) {
        String trimmed = StringUtils.trim(dateTime);
        if (!trimmed.matches("[0-9]{14}"))
            return null;
        return trimmed.substring(0, 4) + "-" + trimmed.substring(4, 6) + "-" + trimmed.substring(6, 8) + " "
                + trimmed.substring(8, 10) + ":" + trimmed.substring(10, 12) + ":" + trimmed.substring(12);
    }

    private boolean validate(String[] lineSplitted) {
        if (lineSplitted[CSV_COLUMN_NUMBER - 1].isEmpty())
            return false;
        for (int column : REQUIRED_COLUMNS_NUMBERS) {
            if (lineSplitted[column].isEmpty())
                return false;
        }

        return true;
    }

    private String mapGdeltLine(String[] lineSplitted) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int column : REQUIRED_COLUMNS_NUMBERS) {
            stringBuilder.append(StringUtils.trim(lineSplitted[column]));
            stringBuilder.append('\t');
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }
}
