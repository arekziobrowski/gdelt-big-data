package functions;

import models.ArticleData;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;

public class ArticleDataOperationsTest {

    @Test
    public void mapArticleApiInfoToArticleApiInfoTest() throws Exception {
        String s = "https://www.wsicweb.com/crime/charlotte-man-accused-of-leading-rowan-deputies-on-chase-with-teen-in-car/\t" +
                "2020-01-06 18:45:00\t" +
                "2020-01-06\t" +
                "112\t" +
                "-2.0\t" +
                "2\t" +
                "1\t" +
                "2\t" +
                "-4.09683426443203\t" +
                "4\t" +
                "Salisbury, Mashonaland East, Zimbabwe\t" +
                "ZI";

        ArticleData articleData = new ArticleDataOperations.MapArticleDataToArticleData().call(s);

        Assert.assertEquals("https://www.wsicweb.com/crime/charlotte-man-accused-of-leading-rowan-deputies-on-chase-with-teen-in-car/", articleData.getSourceUrl());
        Assert.assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2020-01-06 18:45:00"), articleData.getDateAdded());
        Assert.assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2020-01-06"), articleData.getEventDate());
        Assert.assertEquals("https://www.wsicweb.com/crime/charlotte-man-accused-of-leading-rowan-deputies-on-chase-with-teen-in-car/", articleData.getSourceUrl());
        Assert.assertEquals("112", articleData.getEventCode());
        Assert.assertEquals(new Double(-4.09683426443203), articleData.getAverageTone());
        Assert.assertEquals("ZI", articleData.getCountryCode());
    }
}
