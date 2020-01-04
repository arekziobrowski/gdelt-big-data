package functions;


import model.JsonEntry;
import model.JsonEntryProcessed;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JsonEntryProcessedMapFunctionTest {
    private JsonEntryProcessedMapFunction jsonEntryProcessedMapFunction;

    @Before
    public void setUp() {
        this.jsonEntryProcessedMapFunction = new JsonEntryProcessedMapFunction();
    }

    @Test
    public void mappingTest() throws Exception {
        JsonEntry jsonEntry = getEntry();
        final JsonEntryProcessed output = jsonEntryProcessedMapFunction.call(jsonEntry);
        assertEntryProccessedBesideSeenDate(output);
        Assert.assertEquals("2020-01-03 12:13:34", output.getSeenDate());
    }

    @Test
    public void testValidationDateTest() throws Exception {
        JsonEntry jsonEntry = getEntry();
        jsonEntry.withSeenDate(jsonEntry.getSeenDate() + "X");
        final JsonEntryProcessed output = jsonEntryProcessedMapFunction.call(jsonEntry);
        assertEntryProccessedBesideSeenDate(output);
        Assert.assertEquals("", output.getSeenDate());
    }

    @Test
    public void testValidationDateTest2() throws Exception {
        JsonEntry jsonEntry = getEntry();
        jsonEntry.withSeenDate(jsonEntry.getSeenDate().replace("T", " "));
        final JsonEntryProcessed output = jsonEntryProcessedMapFunction.call(jsonEntry);
        assertEntryProccessedBesideSeenDate(output);
        Assert.assertEquals("", output.getSeenDate());
    }

    @Test
    public void trimTest() throws Exception {
        JsonEntry jsonEntry = getEntry();
        jsonEntry.withSeenDate("  " + jsonEntry.getSeenDate() + "  ");
        jsonEntry.withDomain(" " + jsonEntry.getDomain() + "\t");
        jsonEntry.withLanguage(jsonEntry.getLanguage() + "\t");
        final JsonEntryProcessed output = jsonEntryProcessedMapFunction.call(jsonEntry);
        assertEntryProccessedBesideSeenDate(output);
        Assert.assertEquals("2020-01-03 12:13:34", output.getSeenDate());
    }

    private JsonEntry getEntry() {
        JsonEntry jsonEntry = new JsonEntry();
        return jsonEntry.withDomain("domain")
                .withLanguage("language")
                .withSeenDate("20200103T121334Z")
                .withSocialImage("social image")
                .withSourceCountry(null)
                .withTitle("")
                .withUrl("URL1")
                .withUrlMobile("URL2")
                .withArticlePath("article path")
                .withImagePath("image path");
    }

    private void assertEntryProccessedBesideSeenDate(JsonEntryProcessed jsonEntryProcessed) {
        Assert.assertEquals("image path", jsonEntryProcessed.getImagePath());
        Assert.assertEquals("language", jsonEntryProcessed.getLanguage());
        Assert.assertNull(jsonEntryProcessed.getSourceCountry());
        Assert.assertEquals("article path", jsonEntryProcessed.getTextPath());
        Assert.assertEquals("", jsonEntryProcessed.getTitle());
        Assert.assertEquals("URL1", jsonEntryProcessed.getUrl());
    }
}