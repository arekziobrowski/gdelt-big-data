import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;



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

        JavaRDD<String> paths = sc.parallelize(Arrays.asList("src/main/resources/test"));
        JavaRDD<ImageMetadata> ims = paths.flatMap(new FlatMapFunction<String, ImageMetadata>() {
            @Override
            public Iterable<ImageMetadata> call(String s) throws Exception {
                HashMap<Color, ImageMetadata> colorMap = new HashMap<>();
                BufferedImage image = ImageIO.read(new File(s));

                if (image != null) {
                    for (int x = 0; x < image.getWidth(); x++) {
                        for (int y = 0; y < image.getHeight(); y++) {
                            Color c = new Color(image.getRGB(x, y));
                            if (colorMap.containsKey(c)) {
                                colorMap.get(c).incrementCount();
                            }
                            else {
                                colorMap.put(c, new ImageMetadata(c, 1, 1, new Date()));
                            }
                        }
                    }
                }

                return colorMap.values();
            }
        });

        for (ImageMetadata i : ims.collect()) {
            Color c = i.getC();
            System.out.println("R:" + c.getRed() + ", G: " + c.getGreen() + ", B: " + c.getBlue() + ", count: " + i.getCount());
        }

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
