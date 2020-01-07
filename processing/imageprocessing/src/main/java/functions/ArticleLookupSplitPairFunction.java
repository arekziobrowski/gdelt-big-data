package functions;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class ArticleLookupSplitPairFunction implements PairFunction<String, String, Integer> {

    private static final String DELIMITER = "\t";

    @Override
    public Tuple2<String, Integer> call(String s) throws Exception {
        String[] spl = s.split(DELIMITER);
        return new Tuple2<>(spl[1], Integer.parseInt(spl[0]));
    }
}
