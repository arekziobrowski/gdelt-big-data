package functions;

import model.CameoCountry;
import org.junit.Assert;
import org.junit.Test;

public class MapCameoCountryToCameoCountryTest {

    @Test
    public void mapCameoCountryToCameoCountryTestNotNull() throws Exception {
        String s = "AF\tAfghanistan";

        CameoCountry cameoCountry = new MapCameoCountryToCameoCountry().call(s);

        Assert.assertEquals("AF", cameoCountry.getId());
        Assert.assertEquals("Afghanistan", cameoCountry.getName());

    }

    @Test
    public void mapCameoCountryToCameoCountryTestNull() throws Exception {
        String s = " -\tZaire";

        CameoCountry cameoCountry = new MapCameoCountryToCameoCountry().call(s);

        Assert.assertNull(cameoCountry);
    }
}
