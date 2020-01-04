import java.awt.*;
import java.sql.Timestamp;
import java.util.Date;

public class ImageMetadata {

    private Integer count;
    private Color c;
    private Integer colorId;
    private Integer imageId;
    private Date loadDate;

    public ImageMetadata() {
    }

    public ImageMetadata(Integer count, Color color, Integer colorId, Integer imageId, Date loadDate) {
        this.count = count;
        this.c = color;
        this.colorId = colorId;
        this.imageId = imageId;
        this.loadDate = loadDate;
    }

    public Integer getCount() {
        return count;
    }

    public Color getC() {
        return c;
    }

    public Integer getColorId() {
        return colorId;
    }

    public Integer getImageId() {
        return imageId;
    }

    public Date getLoadDate() {
        return loadDate;
    }
}
