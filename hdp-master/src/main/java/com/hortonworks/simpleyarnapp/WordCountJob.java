package com.hortonworks.simpleyarnapp;

import org.apache.hadoop.fs.Path;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Aliaksei_Neuski on 8/31/16.
 */
public class WordCountJob {
    private List<String> dataLines;
    private String dataFilePath;
    private int startNum;
    private int endNum;

    public static void main(String[] args) {
        final String dataFilePath = args[0];
        final int startNum = Integer.valueOf(args[1]);
        final int endNum = Integer.valueOf(args[2]);

        WordCountJob helloYarn = new WordCountJob(dataFilePath, startNum, endNum);
        helloYarn.doJob();
    }

    private WordCountJob(String dataFilePath, int startNum, int endNum) {
        System.out.println("[HelloYarn!]");

        this.dataFilePath = dataFilePath;
        this.startNum = startNum;
        this.endNum = endNum;
    }

    private void doJob() {
        readData();
        processData();
    }

    private void readData() {
        //File to read in HDFS
        try {
            dataLines = HDFSHelper.readLines(new Path(Constants.HDFS_ROOT_PATH + dataFilePath));
        } catch (Exception e) {
            System.err.println("Error when reading data from file:\n" + e.getMessage());
        }
    }

    private void processData() {
        System.out.println("[Job] Process from " + startNum + " to " + endNum + " items.");

        List<String> resultLines = new ArrayList<>();
        resultLines.add(dataLines.get(0)); // add header

        for (int i = startNum; i < endNum; ++i) {
            String line = dataLines.get(i);
            String link = TextHelper.extractLink(line);
            String text = HTTPHelper.extractTextFromUrl(link);

            String[] linkTextPair = {link, text};
            if (!linkTextPair[1].isEmpty()) {
                List<String> currentItemTopWords = TextHelper.extractTopWords(linkTextPair[1], 10);
                if (currentItemTopWords.size() > 0) {
                    String topsString = currentItemTopWords.toString().replace("[", "").replace("]", "");
                    resultLines.add(line.replaceFirst("\t", "\t" + topsString + "\t"));
                }
            }
        }

        HDFSHelper.writeLines(resultLines, dataFilePath.replace("alex_dev", "alex_dev/hw2/out") + "_" + System.currentTimeMillis());
    }


}
