package functions;

import model.Phrase;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RakePairMapperTest {
    private RakePairMapper rakePairMapper;

    @Before
    public void setUp() throws Exception {
        InputStream resourceAsStream = RakePairMapperTest.class.getResourceAsStream("/st");
        List<String> keywordsList = IOUtils.readLines(resourceAsStream, StandardCharsets.UTF_8);
        this.rakePairMapper = new RakePairMapper(keywordsList, -1.0); //Becuase of negative value it will return every keyphrase
    }

    @Test
    public void rakeMapperTest() throws Exception {
        String content = "Compatibility of systems of linear constraints over the set of natural numbers. Criteria of compatibility"
                + " of a system of linear Diophantine equations, strict inequations, and nonstrict inequations are considered. Upper"
                + " bounds for components of a minimal set of solutions and algorithms of construction of minimal generating sets of "
                + "solutions for all types of systems are given. These criteria and the corresponding algorithms for constructing a " +
                "minimal supporting set of solutions can be used in solving all the considered types of systems and systems of mixed types.";
        String url = "file:/home/jakub/path/to/aritcle/article.dat";

        String expectedUrl = "/home/jakub/path/to/aritcle/article.dat";
        Map<String, Double> expectedPhrases = prepareMap();

        final Tuple2<String, Phrase[]> output = this.rakePairMapper
                .call(new Tuple2<String, String>(url, content));

        Assert.assertEquals(expectedUrl, output._1());

        List<Phrase> actualPhrases = Arrays.asList(output._2());
        Assert.assertEquals(expectedPhrases.size(), actualPhrases.size());
        for (Phrase phrase : actualPhrases) {
            Assert.assertTrue("Phrase assertions: " + phrase.toString()
                    , expectedPhrases.containsKey(phrase.getKeyWord()));
            Assert.assertEquals("Phrase assertions: " + phrase.toString(),
                    expectedPhrases.get(phrase.getKeyWord()), phrase.getValue(), 0.01);
        }
    }

    private Map<String, Double> prepareMap() {
        Map<String, Double> map = new HashMap<>();
        map.put("minimal generating sets", 8.666666666666666);
        map.put("linear diophantine equations", 8.5);
        map.put("minimal supporting set", 7.666666666666666);
        map.put("minimal set", 4.666666666666666);
        map.put("linear constraints", 4.5);
        map.put("upper bounds", 4.0);
        map.put("natural numbers", 4.0);
        map.put("nonstrict inequations", 4.0);
        map.put("strict inequations", 4.0);
        map.put("mixed types", 3.666666666666667);
        map.put("considered types", 3.166666666666667);
        map.put("set", 2.0);
        map.put("types", 1.6666666666666667);
        map.put("considered", 1.5);
        map.put("constructing", 1.0);
        map.put("solutions", 1.0);
        map.put("solving", 1.0);
        map.put("system", 1.0);
        map.put("compatibility", 1.0);
        map.put("systems", 1.0);
        map.put("criteria", 1.0);
        map.put("construction", 1.0);
        map.put("algorithms", 1.0);
        map.put("components", 1.0);
        return map;
    }
}