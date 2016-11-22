package com.epam.bigdata.task3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.epam.bigdata.task3.jobs.MRVisitsSpendsJob;
import com.epam.bigdata.task3.models.MPVisitSpendDTO;
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
public class MRVisitsSpendsJobTest {

    private MapDriver<Object, Text, Text, MPVisitSpendDTO> mapDriver;
    private ReduceDriver<Text, MPVisitSpendDTO, Text, MPVisitSpendDTO> reduceDriver;
    private MapReduceDriver<Object, Text, Text, MPVisitSpendDTO, Text, MPVisitSpendDTO> mapReduceDriver;

    private final String input1 = "8c66f1538798b7ab57e2da7be11c5696	20130606222224943	Z0KpO7S8PQpNDBa	Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)	111.1.34.*	...	300	250	0	0	100	e1af08818a6cd6bbba118bb54a651961	254	3476	282825712806	0";
    private final String input2 = "8c66f1538798b7ab57e2da7be11c5696	20130606222224943	Z0KpO7S8PQpNDBa	Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)	111.1.34.*	...	300	250	0	0	100	e1af08818a6cd6bbba118bb54a651961	254	3476	282825712806	0";
    private final String input3 = "8c66f1538798b7ab57e2da7be11c5696	20130606222224943	Z0KpO7S8PQpNDBa	Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)	99.2.72.*	...	300	250	0	0	100	e1af08818a6cd6bbba118bb54a651961	333	3476	282825712806	0";

    private final String ip1 = "111.1.34.*";
    private final String ip2 = "99.2.72.*";

    @Before
    public void setUp() {
        MRVisitsSpendsJob.MRVisitsSpendsMapper mapper = new MRVisitsSpendsJob.MRVisitsSpendsMapper();
        MRVisitsSpendsJob.MRVisitsSpendsReducer reducer = new MRVisitsSpendsJob.MRVisitsSpendsReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(input1));
        mapDriver.withInput(new LongWritable(), new Text(input2));
        mapDriver.withInput(new LongWritable(), new Text(input3));
        mapDriver.withOutput(new Text(ip1), new MPVisitSpendDTO(1, 254));
        mapDriver.withOutput(new Text(ip1), new MPVisitSpendDTO(1, 254));
        mapDriver.withOutput(new Text(ip2), new MPVisitSpendDTO(1, 333));

        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<MPVisitSpendDTO> values1 = new ArrayList<>();
        values1.add(new MPVisitSpendDTO(1, 254));
        values1.add(new MPVisitSpendDTO(1, 254));
        reduceDriver.withInput(new Text(ip1), values1);

        List<MPVisitSpendDTO> values2 = new ArrayList<>();
        values2.add(new MPVisitSpendDTO(1, 333));
        reduceDriver.withInput(new Text(ip2), values2);

        reduceDriver.withOutput(new Text(ip1), new MPVisitSpendDTO(2, 508));
        reduceDriver.withOutput(new Text(ip2), new MPVisitSpendDTO(1, 333));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(input1));
        mapReduceDriver.withInput(new LongWritable(), new Text(input2));
        mapReduceDriver.withInput(new LongWritable(), new Text(input3));

        mapReduceDriver.withOutput(new Text(ip1), new MPVisitSpendDTO(2, 508));
        mapReduceDriver.withOutput(new Text(ip2), new MPVisitSpendDTO(1, 333));

        mapReduceDriver.runTest();
    }
}
