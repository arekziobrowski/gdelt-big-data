package functions;

import model.ArticleInfo;
import model.ImageMetadata;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.broadcast.Broadcast;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import utils.Util;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;

public class ImageMetadataComputeFlatMapFunctionTest {

    private static final String HDFS_IMAGE_PATH = "etl/image.png";

    private ImageMetadataComputeFlatMapFunction imageMetadataComputeFlatMapFunction;

    @Before
    public void setUp() throws IOException {
        this.imageMetadataComputeFlatMapFunction = new ImageMetadataComputeFlatMapFunction(getArticleLookup());
        FileSystem fileSystem = FileSystem.newInstance(Util.getSc().hadoopConfiguration());
        FSDataOutputStream fs = fileSystem.create(new Path(HDFS_IMAGE_PATH));

        BufferedImage image = ImageIO.read(new File("src/test/resources/black-1.png"));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(image, "png", baos);
        byte[] bytes = baos.toByteArray();
        fs.write(bytes);
        fs.close();

        JedisPool jedisPool = Mockito.mock(JedisPool.class);
        Jedis jedis = Mockito.mock(Jedis.class);
        Mockito.when(jedis.get("0,0,0")).thenReturn("1");
        Mockito.when(jedisPool.getResource()).thenReturn(jedis);
        Util.setJedis(jedisPool);
    }

    @After
    public void tearDown() throws IOException {
        FileSystem fileSystem = FileSystem.newInstance(Util.getSc().hadoopConfiguration());
        fileSystem.delete(new Path("etl"), true);
        fileSystem.close();
    }


    @Test
    public void mappingTest() throws Exception {
        ArticleInfo articleInfo = getArticleInfo();
        Iterable<ImageMetadata> out = imageMetadataComputeFlatMapFunction.call(articleInfo);
        List<ImageMetadata> imageMetadata = new ArrayList<>();
        for (ImageMetadata i : out) {
            imageMetadata.add(i);
        }
        Assert.assertEquals(1, imageMetadata.size());

        ImageMetadata i = imageMetadata.get(0);
        Assert.assertEquals(new Integer(1), i.getCount());
        Assert.assertEquals(new Integer(1), i.getColorId());
        Assert.assertEquals(new Integer(1), i.getArticleId());
    }

    public ArticleInfo getArticleInfo() {
        return
                new ArticleInfo(
                        "https://ca.news.yahoo.com/pneumonia-outbreak-china-spurs-fever-013226748.html",
                        "Example title",
                        new Date(),
                        "EN",
                        "China",
                        HDFS_IMAGE_PATH,
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
