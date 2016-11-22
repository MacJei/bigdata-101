package com.epam.bigdata.spark;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import org.apache.spark.streaming.kafka.KafkaUtils;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by Aliaksei_Neuski on 10/7/16.
 */
public class Driver {

    private static final Configuration HBASE_CONF;

    private static SimpleDateFormat tmsFormatter = new SimpleDateFormat("yyyyMMddhhmmss");
    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final Pattern SPACE = Pattern.compile(" ");
    private static final String SPLIT = "\\t";
    private static final String NULL = "null";
    private static final String BID_ID = "bid_Id";
    private static final String TIMESTAMP_DATE = "timestamp_data";
    private static final String IPINYOU_ID = "ipinyou_Id";
    private static final String USER_AGENT = "user_agent";
    private static final String IP = "ip";
    private static final String REGION = "region";
    private static final String CITY ="city";
    private static final String AD_EXCHANGE = "ad_exchange";
    private static final String DOMAIN = "domain";
    private static final String URL = "url";
    private static final String AU_ID = "anonymous_url_id";
    private static final String AS_ID = "ad_slot_id";
    private static final String AS_WIDTH = "ad_slot_width";
    private static final String AS_HEIGHT = "ad_slot_height";
    private static final String AS_VISIBILITY = "ad_slot_visibility";
    private static final String AS_FORMAT = "ad_slot_format";
    private static final String P_PRICE = "paying_price";
    private static final String CREATIVE_ID = "creative_id";
    private static final String B_PRICE = "bidding_price";
    private static final String ADV_ID = "advertiser_id";
    private static final String USER_TAGS = "user_tags";
    private static final String STREAM_ID = "stream_id";
    private static final String DEVICE = "device";
    private static final String OS = "os_name";

    /*
     * Initialization
     */
    static {
        HBASE_CONF = HBaseConfiguration.create();
        HBASE_CONF.set("hbase.zookeeper.property.clientPort", "2181");
        HBASE_CONF.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
        HBASE_CONF.set("zookeeper.znode.parent", "/hbase-unsecure");
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: Driver {zkQuorum} {group} {topic} {numThreads}");
            System.exit(1);
        }

        String zkQuorum = args[0];
        String group = args[1];
        String topic = args[2];
        int numThreads = Integer.parseInt(args[3]);

        SparkConf sparkConf = new SparkConf().setAppName("Spark 9");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
        jssc.checkpoint("hdfs://sandbox.hortonworks.com/tmp/alex_dev/spark/checkpoint");

        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(topic, numThreads);

        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);
        messages.checkpoint(Durations.seconds(100));

        // Test
//        final HBaseAdmin hBaseAdmin = new HBaseAdmin(HBASE_CONF);
//
//        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf("TABLE_NAME"));
//        descriptor.addFamily(new HColumnDescriptor("COLUMN_FAMILY_NAME"));
//
//        System.out.println("Create table " + descriptor.getNameAsString());
//        hBaseAdmin.createTable(descriptor);

        JavaDStream<String> lines = messages.map(tuple2 -> {

            // Initialization HBase Configuration
            final Configuration hbaseConf = HBaseConfiguration.create();
            hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
            hbaseConf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
            hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure");

            // HBase Data
            final String logLine = tuple2._2();

            // iPinyou ID(*) + Timestamp
            String rowKey = logLine.split("\\t")[2] + "_" + logLine.split("\\t")[1];

            // Split each line into fields
            String[] fields = tuple2._2().split(SPLIT);

            UserAgent ua = UserAgent.parseUserAgentString(fields[3]);
            String device =  ua.getBrowser() != null ? ua.getOperatingSystem().getDeviceType().getName() : null;
            String osName = ua.getBrowser() != null ? ua.getOperatingSystem().getName() : null;

            /* ***************************************
            * DWH backup storage with raw data
            * ****************************************
            */

            String table = "LOGS";
            String columnFamily = "logs";
            if (!NULL.equals(fields[2])) {
                // Instantiating HTable class
                final HTable logTable = new HTable(hbaseConf, table);

                // Instantiating Put class accepts a row name.
                Put put = new Put(Bytes.toBytes(rowKey));

                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(BID_ID), Bytes.toBytes(fields[0]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(TIMESTAMP_DATE), Bytes.toBytes(formatter.format(tmsFormatter.parse(fields[1]))));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(IPINYOU_ID), Bytes.toBytes(fields[2]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(USER_AGENT), Bytes.toBytes(fields[3]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(IP), Bytes.toBytes(fields[4]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(REGION), Bytes.toBytes(Integer.parseInt(fields[5])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(CITY), Bytes.toBytes(Integer.parseInt(fields[6])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(AD_EXCHANGE), Bytes.toBytes(Integer.parseInt(fields[7])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(DOMAIN), Bytes.toBytes(fields[8]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(URL), Bytes.toBytes(fields[9]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(AU_ID), Bytes.toBytes(fields[10]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(AS_ID), Bytes.toBytes(fields[11]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(AS_WIDTH), Bytes.toBytes(Integer.parseInt(fields[12])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(AS_HEIGHT), Bytes.toBytes(Integer.parseInt(fields[13])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(AS_VISIBILITY), Bytes.toBytes(Integer.parseInt(fields[14])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(AS_FORMAT), Bytes.toBytes(Integer.parseInt(fields[15])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(P_PRICE), Bytes.toBytes(Integer.parseInt(fields[16])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(CREATIVE_ID), Bytes.toBytes(fields[17]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(B_PRICE), Bytes.toBytes(Integer.parseInt(fields[18])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(ADV_ID), Bytes.toBytes(Integer.parseInt(fields[19])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(USER_TAGS), Bytes.toBytes(Long.parseLong(fields[20])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(STREAM_ID), Bytes.toBytes(Integer.parseInt(fields[21])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(DEVICE), Bytes.toBytes(device));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(OS), Bytes.toBytes(osName));

                try {
                    logTable.put(put);
                } catch (IOException e) {
                    System.out.println("IOException" + e.getMessage());
                }

                System.out.println("Write to table: " + tuple2.toString());

                // Flush commits and close the table
                logTable.flushCommits();
                logTable.close();
            }

            // Adding values using add() method
            // Accepts column family name, qualifier/row name, value
            // The column family must already exist in table schema.
            // The qualifier can be anything.
//            logPut.add(Bytes.toBytes("logs"), Bytes.toBytes(topic), Bytes.toBytes(logLine));
//            try {
//                // Saving the put Instance to the HTable.
//                logTable.put(logPut);
//            } catch (IOException e) {
//                System.err.println(e.getMessage());
//            }

            /* ***************************************
            * Session hot row cache
            * ****************************************
            */

            final HTable ttlTable = new HTable(hbaseConf, topic + "_ttl");
            Put ttlPut = new Put(Bytes.toBytes(rowKey));
            ttlPut.add(Bytes.toBytes("logs"), Bytes.toBytes(topic + "_ttl"), Bytes.toBytes(logLine));
            try {
                ttlTable.put(ttlPut);
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }

            return logLine;
        });

        // Words Count MR
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(Pattern.compile("\\t").split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
    }

    /**
     * Create a table
     */
    public static void creatTable(String tableName, String[] familys) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(HBASE_CONF);
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (String family : familys) {
                tableDesc.addFamily(new HColumnDescriptor(family));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
    }

    /**
     * Delete a table
     */
    public static void deleteTable(String tableName) throws Exception {
        try {
            HBaseAdmin admin = new HBaseAdmin(HBASE_CONF);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
        } catch (MasterNotRunningException | ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }

    /**
     * Put (or insert) a row
     */
    public static void addRecord(String tableName, String rowKey, String family, String qualifier, String value) throws Exception {
        try {
            HTable table = new HTable(HBASE_CONF, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table " + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Delete a row
     */
    public static void delRecord(String tableName, String rowKey) throws IOException {
        HTable table = new HTable(HBASE_CONF, tableName);
        List<Delete> list = new ArrayList<>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        System.out.println("del recored " + rowKey + " ok.");
    }

    /**
     * Get a row
     */
    public static void getOneRecord (String tableName, String rowKey) throws IOException{
        HTable table = new HTable(HBASE_CONF, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for(KeyValue kv : rs.raw()){
            System.out.print(new String(kv.getRow()) + " " );
            System.out.print(new String(kv.getFamily()) + ":" );
            System.out.print(new String(kv.getQualifier()) + " " );
            System.out.print(kv.getTimestamp() + " " );
            System.out.println(new String(kv.getValue()));
        }
    }
    /**
     * Scan (or list) a table
     */
    public static void getAllRecord (String tableName) {
        try{
            HTable table = new HTable(HBASE_CONF, tableName);
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            for(Result r : ss) {
                for(KeyValue kv : r.raw()){
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                }
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }
}
