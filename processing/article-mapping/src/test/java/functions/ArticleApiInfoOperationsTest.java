package functions;

import models.ArticleApiInfo;
import org.junit.Assert;
import org.junit.Test;

public class ArticleApiInfoOperationsTest {

    @Test
    public void mapArticleApiInfoToArticleApiInfoTest() throws Exception {
        String s = "https://ca.news.yahoo.com/vietnam-anti-war-drama-premiere-130000779.html\t" +
                "New Vietnam anti - war drama to premiere on CBC\t" +
                "2020-01-06 23:45:00\t" +
                "English\t" +
                "United States\t" +
                "/etl/staging/cleansed/20200106/images/e6026f31b7bbd64ab19ce75b381ee55f\t" +
                "/etl/staging/cleansed/20200106/texts/c881eadf659c3a6292256ae697fe24ea";

        ArticleApiInfo articleApiInfo = new ArticleApiInfoOperations.MapArticleApiInfoToArticleApiInfo().call(s);

        Assert.assertEquals("https://ca.news.yahoo.com/vietnam-anti-war-drama-premiere-130000779.html", articleApiInfo.getUrl());
        Assert.assertEquals("New Vietnam anti - war drama to premiere on CBC", articleApiInfo.getTitle());
        Assert.assertEquals("English", articleApiInfo.getLanguage());
    }
}
