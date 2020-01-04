import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.input.PortableDataStream;
import org.apache.tika.Tika;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.*;
import java.util.List;


public class ImagePixelProcessor {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Image processing");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //JavaPairRDD<String, PortableDataStream> image = sc.binaryFiles("src/main/resources/image");
        // Get path
        // Probe type Files.probeContentType((new File("filename.ext")).toPath());
        // Read image
        //
        // Map List of ImageMetadata
        // Map List of articles
        final Tika tika = new Tika();


        JavaRDD<String> paths = sc.parallelize(Arrays.asList("src/main/resources/test"));
        JavaRDD<ImageMetadata> ims = paths.flatMap(new FlatMapFunction<String, ImageMetadata>() {
            @Override
            public Iterable<ImageMetadata> call(String s) throws Exception {
                String mimeType = tika.detect(new File(s));

                return null;
            }
        });

        for (ImageMetadata i : ims.collect()) {
            Color c = i.getC();
            System.out.println("R:" + c.getRed() + ", G: " + c.getGreen() + ", B: " + c.getBlue());
        }
        /*JavaRDD<ImageMetadata> imageMetadataRDD = image.values().map(new Function<PortableDataStream, ImageMetadata>() {
            @Override
            public ImageMetadata call(PortableDataStream portableDataStream) throws Exception {
                BufferedImage bufferedImage = createImageFromBytes(portableDataStream.toArray());

                for (int x = 0; x < bufferedImage.getWidth(); x++) {
                    for (int y = 0; y < bufferedImage.getHeight(); y++) {
                        Color c = new Color(bufferedImage.getRGB(x, y));
                        return new ImageMetadata(1, c, 1, 1, new Date());
                    }
                }

                return null;
            }
        });*/

        /*for (ImageMetadata i : imageMetadataRDD.collect()) {
            Color c = i.getC();
            System.out.println("R:" + c.getRed() + ", G: " + c.getGreen() + ", B: " + c.getBlue());
        }*/

        //System.out.println(image);
    }

    private static BufferedImage createImageFromBytes(byte[] imageData) {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(imageData);
        try {
            return ImageIO.read(byteArrayInputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
