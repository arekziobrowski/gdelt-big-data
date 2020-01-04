import java.io.Serializable;
import java.util.Date;

public class ImageMetadata implements Serializable {

    private Integer count;
    private Color c;
    private Integer colorId;
    private Integer imageId;
    private Date loadDate;

    public ImageMetadata(Color color, Integer colorId, Integer imageId, Date loadDate) {
        this.count = 1;
        this.c = color;
        this.colorId = colorId;
        this.imageId = imageId;
        this.loadDate = loadDate;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public void setC(Color c) {
        this.c = c;
    }

    public void setColorId(Integer colorId) {
        this.colorId = colorId;
    }

    public void setImageId(Integer imageId) {
        this.imageId = imageId;
    }

    public void setLoadDate(Date loadDate) {
        this.loadDate = loadDate;
    }

    public void incrementCount() {
        this.count++;
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
