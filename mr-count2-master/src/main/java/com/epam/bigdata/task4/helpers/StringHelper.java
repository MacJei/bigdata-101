package com.epam.bigdata.task4.helpers;

import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by Aliaksei_Neuski on 9/6/16.
 */
public class StringHelper {

    public static List<String> readFile(Path filePath) {
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));

            List<String> lines = new ArrayList<>();

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                lines.add(line);
            }

            return lines;
        } catch (IOException e) {
            System.err.println("Exception while reading file: " + e.getMessage());
        }

        return Collections.emptyList();
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
