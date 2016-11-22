package com.epam.bigdata.spark;

/**
 * Created by Aliaksei_Neuski on 10/5/16.
 */
public class Driver {

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.printf("Usage: %s [generic options] <db_table> <hdfs_tags_file> <hdfs_city_file> <yes|no> \n", "Driver");
            System.exit(2);
        }

        if (args.length > 4) {
            FacebookGrabber.CLIEN_ID = args[4];
            FacebookGrabber.CLIEN_SECRET = args[5];
            FacebookGrabber.TOKEN = args[6];
        }

        String tblName = args[0];
        String tagsFilePath = args[1];
        String cityFilePath = args[2];
        boolean isUseFullDataset = "yes".equals(args[3]);
        KeywordGrabber.grab(tblName, tagsFilePath, cityFilePath, isUseFullDataset);
    }
}
