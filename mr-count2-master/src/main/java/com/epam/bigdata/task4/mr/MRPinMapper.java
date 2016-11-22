package com.epam.bigdata.task4.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by Aliaksei_Neuski on 9/6/16.
 */
public class MRPinMapper extends Mapper<LongWritable, Text, MRPinCompositeKeyWritable, Text> {
    private Text output = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.length() > 0) {
            output.set(line);

            String attributes[] = line.split("\\t");
            String iPinyouId = attributes[2];

            context.write(new MRPinCompositeKeyWritable(iPinyouId, Long.valueOf(attributes[1])), output);
        }

    }
}
