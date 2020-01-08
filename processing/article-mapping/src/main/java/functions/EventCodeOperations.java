package functions;

import models.EventCode;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class EventCodeOperations {

    public static class MapEventCodeToEventCode implements Function<String, EventCode> {
        @Override
        public EventCode call(String s) {
            String[] spl = s.split("\t", 2);
            EventCode eventCode = null;

            try {
                Integer.parseInt(spl[0]);

                eventCode = new EventCode(
                        spl[0],
                        spl[1]);
            } catch (Exception e) {
                return null;
            }

            return eventCode;
        }
    }

    public static class MapEventCodeToPairFunction implements PairFunction<EventCode, String, EventCode> {
        @Override
        public Tuple2<String, EventCode> call(EventCode inEventCode) {
            return new Tuple2<>(inEventCode.getCameoEventCode(), inEventCode);
        }
    }

    public static class EventCodeNotNullFilter implements Function<EventCode, Boolean> {
        @Override
        public Boolean call(EventCode inEventCode) {
            return !(inEventCode == null);
        }
    }
}
