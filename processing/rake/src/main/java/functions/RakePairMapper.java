package functions;

import model.Phrase;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class RakePairMapper implements PairFunction<Tuple2<String, String>, String, Phrase[]> {
    private final double threshold;
    private final Pattern pattern;
    private final String sentenceSplitRegex = "[.!?,;:\\n\\t\"\\(\\)\'\u2019\u2013]|\\s\\-\\s";
    private final String phraseSplitRegex = "[^a-zA-Z0-9_\\+\\-/]";

    public RakePairMapper(List<String> stopWords, double threshold) {
        this.threshold = threshold;
        this.pattern = Pattern.compile(buildStopWordsRegex(stopWords), Pattern.CASE_INSENSITIVE);
    }

    @Override
    public Tuple2<String, Phrase[]> call(Tuple2<String, String> stringStringTuple2) throws Exception {
        List<String> phrasesString = getPhrasesList(stringStringTuple2._2());
        Map<String, Double> worldsScores = calculateWorldsScores(phrasesString);

        Phrase[] phrases = calculatePhrases(phrasesString, worldsScores, this.threshold).toArray(new Phrase[]{});
        return new Tuple2<>(mapUrl(stringStringTuple2._1()), phrases);
    }

    private String mapUrl(String input) {
        String[] splitted = input.split(":[0-9]*(?=(/[a-zA-Z]))", 2);
        return splitted.length == 2 ? splitted[1] : splitted[0];
    }

    List<Phrase> calculatePhrases(List<String> phrasesList, Map<String, Double> wordScore, double threshold) {
        Map<String, Double> phraseScore = new HashMap<>();
        for (String phrase : phrasesList) {
            if (phraseScore.containsKey(phrase))
                continue;
            double score = 0.0;
            for (String w : separatePhrase(phrase)) {
                score += wordScore.get(w);
            }
            phraseScore.put(phrase, score);
        }

        List<Phrase> phraseList = new LinkedList<>();
        for (Map.Entry<String, Double> entry : phraseScore.entrySet()) {
            if (entry.getValue() > threshold)
                phraseList.add(new Phrase(entry.getKey(), entry.getValue()));
        }
        return phraseList;
    }

    private Map<String, Double> calculateWorldsScores(List<String> phrases) {
        Map<String, Integer> wordFrequency = new HashMap<>();
        Map<String, Integer> wordDegree = new HashMap<>();
        for (String phrase : phrases) {
            List<String> words = separatePhrase(phrase);
            int length = words.size();
            int degree = length - 1;
            for (String word : words) {
                if (wordFrequency.containsKey(word)) {
                    wordFrequency.put(word, wordFrequency.remove(word) + 1);
                } else {
                    wordFrequency.put(word, 1);
                }
                if (wordDegree.containsKey(word)) {
                    wordDegree.put(word, wordDegree.remove(word) + degree);
                } else {
                    wordDegree.put(word, degree);
                }
            }
        }
        for (Map.Entry<String, Integer> freqEntry : wordFrequency.entrySet()) {
            wordDegree.put(freqEntry.getKey(), wordDegree.remove(freqEntry.getKey()) + freqEntry.getValue());
        }
        Map<String, Double> wordScore = new HashMap<>();
        for (Map.Entry<String, Integer> entry : wordFrequency.entrySet()) {
            wordScore.put(entry.getKey(), wordDegree.get(entry.getKey()) / (entry.getValue() * 1.0));
        }
        return wordScore;
    }

    private List<String> separatePhrase(String phrase) {
        List<String> ws = new LinkedList<>();
        for (String s : phrase.split(phraseSplitRegex)) {
            String temp = s.trim();
            if (!temp.matches("[0-9]*"))
                ws.add(temp);
        }
        return ws;
    }

    private List<String> getPhrasesList(String textContent) {
        String[] sentences = textContent.split(sentenceSplitRegex);
        List<String> phrases = new LinkedList<>();
        for (String s : sentences) {
            String[] splittedSentence = pattern.split(s);
            for (String p : splittedSentence) {
                String temp = p.trim().toLowerCase();
                if (!temp.isEmpty()) {
                    phrases.add(temp);
                }
            }
        }
        return phrases;
    }

    private String buildStopWordsRegex(List<String> stopWords) {
        StringBuilder patternBuilder = new StringBuilder();
        for (String stopWord : stopWords) {
            if(StringUtils.trim(stopWord).isEmpty())
                continue;
            patternBuilder.append("\\b");
            patternBuilder.append(stopWord);
            patternBuilder.append("(?![\\w-])|");
        }
        patternBuilder.deleteCharAt(patternBuilder.length() - 1);
        return patternBuilder.toString();
    }
}
