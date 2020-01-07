package functions;

import model.ImageMetadata;
import org.apache.spark.api.java.function.Function;

public class ImageMetadataFilterFunction implements Function<ImageMetadata, Boolean> {

    private static final int PERFORMANCE_THRESHOLD = 10;

    @Override
    public Boolean call(ImageMetadata imageMetadata) throws Exception {
        return imageMetadata.getCount() > PERFORMANCE_THRESHOLD && imageMetadata.getArticleId() != null;
    }
}
