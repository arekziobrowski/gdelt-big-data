import de.l3s.boilerpipe.extractors.ArticleExtractor;

import java.io.FileReader;
import java.net.URL;

public class Extractor {

    public static void main(final String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("You have to specify the content to be parsed!");
            System.err.println("USAGE: java -jar extractor.jar [-url | -file] <ARGUMENT>");
            System.err.println("\t -url URL to be extracted");
            System.err.println("\t -file file from which the text will be extracted");
            System.exit(1);
        }
        if (args[0].equals("-url")) {
            URL url = new URL(args[1]);
            System.out.println(ArticleExtractor.INSTANCE.getText(url));
        }
        else if (args[0].equals("-file")) {
            FileReader fr = new FileReader(args[1]);
            System.out.println(ArticleExtractor.INSTANCE.getText(fr));
        }
        else {
            System.err.println("Incorrect flag!");
            System.exit(1);
        }
    }
}
