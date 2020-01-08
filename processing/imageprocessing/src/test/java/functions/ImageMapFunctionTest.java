package functions;

import model.ArticleInfo;
import model.Image;
import org.apache.spark.broadcast.Broadcast;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import utils.Util;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ImageMapFunctionTest {

    private ImageMapFunction imageMapFunction;

    @Before
    public void setUp() {
        this.imageMapFunction = new ImageMapFunction(getArticleLookup());
    }

    @Test
    public void mappingTest() throws Exception {
        ArticleInfo articleInfo = getArticleInfo();
        final Image image = imageMapFunction.call(articleInfo);
        Assert.assertNull(image.getId());
        Assert.assertEquals("/test/path", image.getUrl());
        Assert.assertEquals(new Integer(1), image.getArticle_id());
        Assert.assertNotNull(image.getLoadDate());
    }

    public ArticleInfo getArticleInfo() {
        return
            new ArticleInfo(
                    "https://ca.news.yahoo.com/pneumonia-outbreak-china-spurs-fever-013226748.html",
                    "Example title",
                    new Date(),
                    "EN",
                    "China",
                    "/test/path",
                    "/test/path"
                );
    }

    public Broadcast<Map<String, Integer>> getArticleLookup() {
        String data = "1\thttps://ca.news.yahoo.com/pneumonia-outbreak-china-spurs-fever-013226748.html";
        Map<String, Integer> map = new HashMap<>();
        map.put(data.split("\t")[1], Integer.parseInt(data.split("\t")[0]));
        return Util.getSc().broadcast(map);
    }
}
