package functions;

import model.OutputRow;
import model.Phrase;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class FlatMapToOutputRowFunction implements FlatMapFunction<Tuple2<Phrase[], String>, OutputRow> {
    private final static String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";

    @Override
    public Iterable<OutputRow> call(Tuple2<Phrase[], String> stringTuple2) throws Exception {
        List<OutputRow> outputRows = new LinkedList<>();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_PATTERN);
        String formattedDate = simpleDateFormat.format(new Date());

        for (Phrase phrase : stringTuple2._1()) {
            outputRows.add(new OutputRow(stringTuple2._2(), phrase.getKeyWord(), phrase.getValue(), formattedDate));
        }
        return outputRows;
    }
}
