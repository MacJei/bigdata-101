package com.epam.bigdata.task4.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Aliaksei_Neuski on 9/6/16.
 */
public class MRPinSecondarySortDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] args1 = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args1.length < 2) {
            System.err.printf("Usage: %s [generic options] <input> <output> <numReduceTasks>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            System.exit(2);
        }

        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
        conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, SnappyCodec.class, CompressionCodec.class);

        Job job = Job.getInstance(conf, "MRPin Secondary Sort");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args1[0]));
        FileOutputFormat.setOutputPath(job, new Path(args1[1]));

        job.setMapperClass(MRPinMapper.class);
        job.setMapOutputKeyClass(MRPinCompositeKeyWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(MRPinPartitioner.class);
        job.setSortComparatorClass(MRPinCompKeySortComparator.class);
        job.setGroupingComparatorClass(MRPinGroupingComparator.class);
        job.setReducerClass(MRPinReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReduceTasks = args1.length == 3 ? Integer.valueOf(args1[2]) : 1;
        job.setNumReduceTasks(numReduceTasks);

        boolean success = job.waitForCompletion(true);

        System.out.println("[Insights]");
        Counters counters = job.getCounters();
        String iPinyouId = "";
        long maxSiteImpression = 0;
        for (Counter counter : counters.getGroup(Insights.class.getName())) {
            System.out.println("[Impression] " + counter.getName() + " : " + counter.getValue());
            if (maxSiteImpression < counter.getValue()) {
                maxSiteImpression = counter.getValue();
                iPinyouId = counter.getName();
            }
        }
        System.out.println("[Max Impression] " + iPinyouId + " : " + maxSiteImpression);

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new MRPinSecondarySortDriver(), args);
        System.exit(exitCode);
    }
}
