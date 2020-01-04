package functions;

import model.JsonEntry;
import model.JsonEntryProcessed;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.Function;

public class JsonEntryProcessedMapFunction implements Function<JsonEntry, JsonEntryProcessed> {
    @Override
    public JsonEntryProcessed call(JsonEntry input) throws Exception {
        return new JsonEntryProcessed().withUrl(input.getUrl())
                .withTitle(StringUtils.trim(input.getTitle()))
                .withSeenDate(handleDateTime(input.getSeenDate()))
                .withLanguage(StringUtils.trim(input.getLanguage()))
                .withSourceCountry(StringUtils.trim(input.getSourceCountry()))
                .withImagePath(StringUtils.trim(input.getImagePath()))
                .withTextPath(StringUtils.trim(input.getArticlePath()));
    }

    private String handleDateTime(String in) {
        String trimmed = StringUtils.trim(in);
        if (trimmed.length() != 16 || trimmed.charAt(8) != 'T' || trimmed.charAt(15) != 'Z')
            return "";
        else {
            return trimmed.substring(0, 4) +
                    '-' +
                    trimmed.substring(4, 6) +
                    '-' +
                    trimmed.substring(6, 8) +
                    ' ' +
                    trimmed.substring(9, 11) +
                    ':' +
                    trimmed.substring(11, 13) +
                    ':' +
                    trimmed.substring(13, 15);
        }
    }
}
