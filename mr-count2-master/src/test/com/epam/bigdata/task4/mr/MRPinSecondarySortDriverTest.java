package com.epam.bigdata.task4.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by Aliaksei_Neuski on 9/7/16.
 */
public class MRPinSecondarySortDriverTest {

    private MapDriver<LongWritable, Text, MRPinCompositeKeyWritable, Text> mapDriver;
    private ReduceDriver<MRPinCompositeKeyWritable, Text, NullWritable, Text> reduceDriver;
    private MapReduceDriver<LongWritable, Text, MRPinCompositeKeyWritable, Text, NullWritable, Text> mapReduceDriver;

    private final String input1 = "f65fce1aab8543534534534534534555	20130607233212992	VhkR1aq4DoCIQOE	Mozilla/5.0 ";
    private final String input2 = "f65fce1arr444d8cdea808fbab2f624b	20130607233212252	VhkR1aq4DoCIQOE	Mozilla/5.0 ";
    private final String input3 = "4444ce1aab865d8cdea808fbab2f624b	20130607233212952	tttR1aq4DoWASA3	Mozilla/5.0 ";
    private final String input4 = "222fce1aab865d8cdea808fbab2f624b	20150607233111152	wrfg56q4DoCIQOE	Mozilla/5.0 ";
    private final String input5 = "165fce1aab865d8cdea808fbab2f624b	20140607233212353	tttR1aq4DoWASA3	Mozilla/5.0 ";

    private final String id1 = "VhkR1aq4DoCIQOE";
    private final long timestamp1 = 20130607233212992L;
    private final long timestamp2 = 20130607233212252L;
    private final String id2 = "tttR1aq4DoWASA3";
    private final long timestamp3 = 20130607233212952L;
    private final long timestamp5 = 20140607233212353L;
    private final String id3 = "wrfg56q4DoCIQOE";
    private final long timestamp4 = 20150607233111152L;


    @Before
    public void setUp() {
        MRPinMapper mapper = new MRPinMapper();
        MRPinReducer reducer = new MRPinReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(input1));
        mapDriver.withInput(new LongWritable(), new Text(input2));
        mapDriver.withInput(new LongWritable(), new Text(input3));
        mapDriver.withInput(new LongWritable(), new Text(input4));
        mapDriver.withInput(new LongWritable(), new Text(input5));
        mapDriver.withOutput(new MRPinCompositeKeyWritable(id1, timestamp1), new Text(input1));
        mapDriver.withOutput(new MRPinCompositeKeyWritable(id1, timestamp2), new Text(input2));
        mapDriver.withOutput(new MRPinCompositeKeyWritable(id2, timestamp3), new Text(input3));
        mapDriver.withOutput(new MRPinCompositeKeyWritable(id3, timestamp4), new Text(input4));
        mapDriver.withOutput(new MRPinCompositeKeyWritable(id2, timestamp5), new Text(input5));

        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<Text> values1 = new ArrayList<>();
        values1.add(new Text(input1));
        List<Text> values2 = new ArrayList<>();
        values2.add(new Text(input2));
        reduceDriver.withInput(new MRPinCompositeKeyWritable(id1, timestamp2), values2);
        reduceDriver.withInput(new MRPinCompositeKeyWritable(id1, timestamp1), values1);

        List<Text> values3 = new ArrayList<>();
        values3.add(new Text(input3));
        List<Text> values5 = new ArrayList<>();
        values5.add(new Text(input5));
        reduceDriver.withInput(new MRPinCompositeKeyWritable(id2, timestamp5), values5);
        reduceDriver.withInput(new MRPinCompositeKeyWritable(id2, timestamp3), values3);

        List<Text> values4 = new ArrayList<>();
        values4.add(new Text(input4));
        reduceDriver.withInput(new MRPinCompositeKeyWritable(id3, timestamp4), values4);

        reduceDriver.withOutput(NullWritable.get(), new Text(input2));
        reduceDriver.withOutput(NullWritable.get(), new Text(input1));
        reduceDriver.withOutput(NullWritable.get(), new Text(input5));
        reduceDriver.withOutput(NullWritable.get(), new Text(input3));
        reduceDriver.withOutput(NullWritable.get(), new Text(input4));

        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(input1));
        mapReduceDriver.withInput(new LongWritable(), new Text(input2));
        mapReduceDriver.withInput(new LongWritable(), new Text(input3));
        mapReduceDriver.withInput(new LongWritable(), new Text(input4));
        mapReduceDriver.withInput(new LongWritable(), new Text(input5));

        mapReduceDriver.withOutput(NullWritable.get(), new Text(input2));
        mapReduceDriver.withOutput(NullWritable.get(), new Text(input1));
        mapReduceDriver.withOutput(NullWritable.get(), new Text(input3));
        mapReduceDriver.withOutput(NullWritable.get(), new Text(input5));
        mapReduceDriver.withOutput(NullWritable.get(), new Text(input4));

        mapReduceDriver.runTest();
    }
}
