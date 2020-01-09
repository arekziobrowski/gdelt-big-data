package functions;

import model.CameoCountry;
import org.apache.spark.api.java.function.Function;

public class MapCameoCountryToCameoCountry implements Function<String, CameoCountry> {
    private static boolean containsOnlyCapitalLetters(String s) {
        return s.matches("[A-Z]+");
    }

    @Override
    public CameoCountry call(String s) {
        String[] spl = s.split("\t", 7);
        CameoCountry cameoCountry = null;

        try {
            cameoCountry = new CameoCountry(
                    spl[0],
                    spl[1]
            );

            if (!containsOnlyCapitalLetters(spl[0])) throw new Exception();

        } catch (Exception e) {
            return null;
        }

        return cameoCountry;
    }
}
