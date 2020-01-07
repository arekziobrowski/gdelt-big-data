package functions;

import model.ArticleInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import java.text.SimpleDateFormat;

public class ArticleInfoSplitFunctionTest {

    private ArticleInfoSplitFunction articleInfoSplitFunction;

    @Before
    public void setUp() {
        this.articleInfoSplitFunction = new ArticleInfoSplitFunction();
    }

    @Test
    public void mappingTest() throws Exception {
        String testData = createData();
        final ArticleInfo articleInfo = articleInfoSplitFunction.call(testData);
        Assert.assertEquals("https://example.com", articleInfo.getUrl());
        Assert.assertEquals("Example title", articleInfo.getTitle());
        Assert.assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2020-01-01 00:00:30"), articleInfo.getSendDate());
        Assert.assertEquals("EN", articleInfo.getLanguage());
        Assert.assertEquals("England", articleInfo.getSourceCountry());
        Assert.assertEquals("/example/path/to/image", articleInfo.getImagePath());
        Assert.assertEquals("/example/path/to/text", articleInfo.getTextPath());
    }

    private String createData() {
        return "https://example.com" + "\t"
                + "Example title" + "\t"
                + "2020-01-01 00:00:30" + "\t"
                + "EN" + "\t"
                + "England" + "\t"
                + "/example/path/to/image" + "\t"
                + "/example/path/to/text" + "\t";
    }
}
