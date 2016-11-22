package com.hortonworks.simpleyarnapp;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;

/**
 * Created by Aliaksei_Neuski on 9/1/16.
 */
public class HTTPHelper {

    public static String extractTextFromUrl(String url) {
        Document doc;
        try {
            StringBuilder sb = new StringBuilder();
            if (null != url && !url.isEmpty()) {
                doc = Jsoup.connect(url).timeout(5000).get();
                Elements children = doc.body().children();
                for (Element child : children) {
                    String text = child.text();
                    if (!text.contains("div")) {
                        sb.append(text);
                        sb.append(" ");
                    }
                    else {
                        text = Jsoup.parse(child.outerHtml()).text();
                        if (!text.contains("div")) {
                            sb.append(text);
                            sb.append(" ");
                        }
                    }
                }
            }
            return sb.toString();
        } catch (IOException e) {
            System.err.println("###");
            System.err.println(url);
            System.err.println(e.getLocalizedMessage());

            String words = TextHelper.extractWordsFromUrl(url);
            System.err.println("Catch to: " + words);

            return words;
        }
    }
}
