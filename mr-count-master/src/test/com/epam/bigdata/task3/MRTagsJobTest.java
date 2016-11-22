package com.epam.bigdata.task3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.epam.bigdata.task3.jobs.MRTagsJob;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.Before;
import org.junit.Test;

/**
 * Created by Aliaksei_Neuski on 9/2/16.
 */
public class MRTagsJobTest {

    private MapDriver<Object, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    private final String input1 = "282163094589 apple,juice,milk ON CPC BROAD https://google.com";
    private final String input2 = "282163091245 apple,juice ON CPC BROAD https://google.com";
    private final String input3 = "282163095555 milk ON CPC BROAD https://google.com";

    private final String s1 = "apple".toUpperCase();
    private final String s2 = "juice".toUpperCase();
    private final String s3 = "milk".toUpperCase();

    @Before
    public void setUp() {
        MRTagsJob.MRTagsMapper mapper = new MRTagsJob.MRTagsMapper();
        MRTagsJob.MRTagsReducer reducer = new MRTagsJob.MRTagsReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(input1));
        mapDriver.withInput(new LongWritable(), new Text(input2));
        mapDriver.withInput(new LongWritable(), new Text(input3));
        mapDriver.withOutput(new Text(s1), new IntWritable(1)); // apple - input 1
        mapDriver.withOutput(new Text(s2), new IntWritable(1)); // juice - input 1
        mapDriver.withOutput(new Text(s3), new IntWritable(1)); // milk - input 1
        mapDriver.withOutput(new Text(s1), new IntWritable(1)); // apple - input 2
        mapDriver.withOutput(new Text(s2), new IntWritable(1)); // juice - input 2
        mapDriver.withOutput(new Text(s3), new IntWritable(1)); // milk - input 3
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values1 = new ArrayList<>();
        values1.add(new IntWritable(1));
        reduceDriver.withInput(new Text(s1), values1);

        List<IntWritable> values2 = new ArrayList<>();
        values2.add(new IntWritable(1));
        values2.add(new IntWritable(1));
        reduceDriver.withInput(new Text(s2), values2);

        List<IntWritable> values3 = new ArrayList<>();
        values3.add(new IntWritable(1));
        values3.add(new IntWritable(1));
        values3.add(new IntWritable(1));
        reduceDriver.withInput(new Text(s3), values3);

        reduceDriver.withOutput(new Text(s1), new IntWritable(1));
        reduceDriver.withOutput(new Text(s2), new IntWritable(2));
        reduceDriver.withOutput(new Text(s3), new IntWritable(3));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new IntWritable(), new Text(input1));
        mapReduceDriver.withInput(new IntWritable(), new Text(input2));
        mapReduceDriver.withInput(new IntWritable(), new Text(input3));

        mapReduceDriver.withOutput(new Text(s1), new IntWritable(2)); // apple x 2
        mapReduceDriver.withOutput(new Text(s2), new IntWritable(2)); // juice x 2
        mapReduceDriver.withOutput(new Text(s3), new IntWritable(2)); // milk x 2

        mapReduceDriver.runTest();
    }
}
