package model;

import java.io.Serializable;
import java.util.Objects;

public class Phrase implements Serializable {
    private static final long serialVersionUID = 1L;

    private String keyWord;
    private double value;

    public Phrase() {

    }

    public Phrase(String keyWord, double value) {
        this.keyWord = keyWord;
        this.value = value;
    }

    public String getKeyWord() {
        return keyWord;
    }

    public void setKeyWord(String keyWord) {
        this.keyWord = keyWord;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Keyword{" +
                "keyWord='" + keyWord + '\'' +
                ", value=" + value +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Phrase phrase = (Phrase) o;
        return Double.compare(phrase.value, value) == 0 &&
                Objects.equals(keyWord, phrase.keyWord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyWord, value);
    }
}
