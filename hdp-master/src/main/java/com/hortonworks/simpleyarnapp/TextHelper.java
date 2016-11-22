package com.hortonworks.simpleyarnapp;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/**
 * Created by Aliaksei_Neuski on 9/1/16.
 */
public class TextHelper {

    public static List<String> extractTopWords(String text, int num) {
        List<String> allWords = Pattern.compile("\\W").splitAsStream(text)
                .filter((s -> !s.isEmpty()))
                .filter(w -> !Pattern.compile("\\d+").matcher(w).matches())
                .collect(toList());

        List<String> topWords = allWords.stream()
                .map(String::toLowerCase)
                .collect(groupingBy(Function.identity(), counting()))
                .entrySet().stream()
                .sorted(Map.Entry.<String, Long> comparingByValue(reverseOrder()).thenComparing(Map.Entry.comparingByKey()))
                .limit(num + Constants.STOP_WORDS.size())
                .map(Map.Entry::getKey)
                .collect(toList());

        topWords = topWords.stream().filter(word -> !Constants.STOP_WORDS.contains(word)).limit(num).collect(toList());

        return topWords;
    }

    public static String extractLink(String text) {
        String link = "";

        String[] lines = text.split("http");
        if (lines.length > 1) {
            link = "http" + lines[1].trim();

            System.out.println(link);
        }

        return link;
    }

    public static String extractWordsFromUrl(String link) {
        return link.split(".com/")[1].replace(".html", "").replace("-", " ");
    }
}
