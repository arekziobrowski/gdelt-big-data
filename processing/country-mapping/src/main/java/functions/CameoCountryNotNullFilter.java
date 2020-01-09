package functions;

import model.CameoCountry;
import org.apache.spark.api.java.function.Function;

public class CameoCountryNotNullFilter implements Function<CameoCountry, Boolean> {
    @Override
    public Boolean call(CameoCountry cameoCountry) {
        return !(cameoCountry == null);
    }
}
