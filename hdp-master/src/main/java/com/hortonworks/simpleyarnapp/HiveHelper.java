package com.hortonworks.simpleyarnapp;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Aliaksei_Neuski on 9/1/16.
 */
public class HiveHelper {

    static {
        try {
            Class.forName(Constants.HIVE_JDBC_DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            System.err.println(e.getLocalizedMessage());
        }
    }

    public static int countAll() throws SQLException {
        Connection con = DriverManager.getConnection(Constants.HIVE_DEFAULT_DB_NAME, "hive", "");
        Statement stmt = con.createStatement();

        String sql = "select count(1) from " + Constants.HIVE_DEFAULT_TABLE_NAME;
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            return res.getInt(1);
        }

        return 0;
    }

    public static List<String> selectPidAndUrl(int from, int to) throws SQLException {
        Connection con = DriverManager.getConnection(Constants.HIVE_DEFAULT_DB_NAME, "hive", "");
        Statement stmt = con.createStatement();

        List<String> result = new ArrayList<>(to - from);

        // select * query
        String sql = "select pid, destination_url from "
                + Constants.HIVE_DEFAULT_TABLE_NAME + " offset " + from + " limit " + (to - from);
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            result.add(String.valueOf(res.getLong(1) + "," + res.getString(2)));
        }

        return result;
    }

    public static void updateTags(String value, long id) throws SQLException {
        Connection con = DriverManager.getConnection(Constants.HIVE_DEFAULT_DB_NAME, "hive", "");
        Statement stmt = con.createStatement();

        String sql = "update " + Constants.HIVE_DEFAULT_TABLE_NAME + " set keyword_value = '" + value + "' WHERE pid = " + id;
        System.out.println("Running: " + sql);

        stmt.executeQuery(sql);
    }

    /*
    Connection con = DriverManager.getConnection(Constants.HIVE_DEFAULT_DB_NAME, "hive", "");
        Statement stmt = con.createStatement();

//        stmt.execute("drop table if exists " + tableName);
//        stmt.execute("create table " + tableName + " (key int, value string)");
        // show tables
//        String sql = "show tables '" + tableName + "'";
//        System.out.println("Running: " + sql);
//        ResultSet res = stmt.executeQuery(sql);
//        if (res.next()) {
//            System.out.println(res.getString(1));
//        }
        // describe table
//        sql = "describe " + tableName;
//        System.out.println("Running: " + sql);
//        res = stmt.executeQuery(sql);
//        while (res.next()) {
//            System.out.println(res.getString(1) + "\t" + res.getString(2));
//        }

        // load data into table
        // NOTE: filepath has to be local to the hive server
        // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
//        String filepath = "/tmp/a.txt";
//        sql = "load data local inpath '" + filepath + "' into table " + tableName;
//        System.out.println("Running: " + sql);
//        stmt.execute(sql);

        // select * query
        String sql = "select pid, destination_url from " + Constants.HIVE_DEFAULT_TABLE_NAME + " limit " + (to - from);
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getLong(1) + "\t" + res.getString(2)));
        }

        // regular hive query
//        sql = "select count(1) from " + tableName;
//        System.out.println("Running: " + sql);
//        res = stmt.executeQuery(sql);
//        while (res.next()) {
//            System.out.println(res.getString(1));
//        }
     */
}
