package com.epam.bigdata.task3.jobs;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import com.epam.bigdata.task3.helpers.StringHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by Aliaksei_Neuski on 9/2/16.
 */
public class MRTagsJob {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] args1 = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args1.length < 2) {
            System.err.println("Usage: MRTagsJob <in> <out> [<in>...]");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "MapReduce Tags Job");
        job.setJarByClass(MRTagsJob.class);
        job.setMapperClass(MRTagsMapper.class);
        job.setCombinerClass(MRTagsReducer.class);
        job.setReducerClass(MRTagsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        if (args1.length > 2) {
            // distributed cache for ignore words
            job.addCacheFile(new Path(args1[2]).toUri());
        }

        FileInputFormat.addInputPath(job, new Path(args1[0]));
        FileOutputFormat.setOutputPath(job, new Path(args1[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MRTagsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable numberOneWritable = new IntWritable(1);
        private Text tag = new Text();
        private Set<String> ignore;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                Path[] localCacheFiles = context.getLocalCacheFiles();
                if (localCacheFiles != null && localCacheFiles.length > 0) {
                    for (Path stopWordFile : localCacheFiles) {
                        ignore = StringHelper.readFile(stopWordFile);
                    }
                }
                else {
                    ignore = Collections.emptySet();
                }
            } catch (IOException e) {
                System.err.println("Setup ex: " + e.getMessage());
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (StringHelper.isStringWithDigit(line)) {
                String[] params = line.split("\\s+");
                String[] tags = params[1].toUpperCase().split(",");
                for (String currentTag : tags) {
                    if (!ignore.contains(currentTag)) {
                        tag.set(currentTag);
                        context.write(tag, numberOneWritable);
                    }
                }
            }
        }
    }

    public static class MRTagsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> items, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable item : items) {
                sum += item.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }
}