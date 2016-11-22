package com.epam.bigdata.task3.helpers;

import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Created by Aliaksei_Neuski on 9/2/16.
 */
public class StringHelper {

    public static Set<String> readFile(Path filePath) {
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));

            Set<String> stopWords = new LinkedHashSet<>();

            String stopWord;
            while ((stopWord = bufferedReader.readLine()) != null) {
                stopWords.add(stopWord.toUpperCase());
            }

            return stopWords;
        } catch (IOException ex) {
            System.err.println("Exception while reading stop words file: " + ex.getMessage());
        }

        return Collections.emptySet();
    }

    public static boolean isStringWithDigit(String s) {
        boolean isDigit = false;

        if (s != null && !s.isEmpty()) {
            for (char c : s.toCharArray()) {
                if (isDigit = Character.isDigit(c)) {
                    break;
                }
            }
        }

        return isDigit;
    }
}
