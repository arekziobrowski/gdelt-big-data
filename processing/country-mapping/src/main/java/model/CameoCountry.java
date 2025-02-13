package model;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;


public class CameoCountry implements Serializable {
    private static final long serialVersionUID = 1L;

    private String id;
    private String name;
    private Date loadDate;

    public CameoCountry(String code, String label) {
        this.id = code;
        this.name = label;
        this.loadDate = new Date();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CameoCountry cameoCountry = (CameoCountry) o;
        return Objects.equals(id, cameoCountry.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getLoadDate() {
        return loadDate;
    }

    public void setLoadDate(Date loadDate) {
        this.loadDate = loadDate;
    }

    @Override
    public String toString() {
        return id + '\t' +
                name + '\t' +
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(loadDate);
    }
}
