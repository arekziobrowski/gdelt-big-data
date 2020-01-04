package functions;

import model.JsonEntry;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

import java.util.LinkedList;
import java.util.List;

public class RowToJsonEntryFlatMapFunction implements FlatMapFunction<Row, JsonEntry> {
    @Override
    public Iterable<JsonEntry> call(Row row) throws Exception {
        List<GenericRowWithSchema> genericRows = row.getList(0);
        List<JsonEntry> list = new LinkedList<>();
        for (GenericRowWithSchema o : genericRows) {
            list.add(new JsonEntry()
                    .withDomain(o.getString(0))
                    .withLanguage(o.getString(1))
                    .withSeenDate(o.getString(2))
                    .withSocialImage(o.getString(3))
                    .withSourceCountry(o.getString(4))
                    .withTitle(o.getString(5))
                    .withUrl(o.getString(6))
                    .withUrlMobile(o.getString(7))
            );
        }
        return list;
    }
}
