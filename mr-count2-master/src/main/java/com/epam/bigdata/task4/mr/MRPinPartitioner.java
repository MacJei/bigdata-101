package com.epam.bigdata.task4.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * Created by Aliaksei_Neuski on 9/6/16.
 */
public class MRPinPartitioner extends Partitioner<MRPinCompositeKeyWritable, Text> {

    @Override
    public int getPartition(MRPinCompositeKeyWritable key, Text value, int numReduceTasks) {
        return Math.abs(key.getiPinyouId().hashCode() % numReduceTasks);
    }
}
