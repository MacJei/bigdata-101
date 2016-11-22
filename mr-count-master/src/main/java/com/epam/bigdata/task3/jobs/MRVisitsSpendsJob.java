package com.epam.bigdata.task3.jobs;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.epam.bigdata.task3.models.MPVisitSpendDTO;
import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Counter;
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
public class MRVisitsSpendsJob {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] args1 = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args1.length < 2) {
            System.err.println("Usage: MRVisitsSpendsJob <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "MapReduce Visits Spends Job");
        job.setJarByClass(MRVisitsSpendsJob.class);
        job.setMapperClass(MRVisitsSpendsMapper.class);
        job.setCombinerClass(MRVisitsSpendsReducer.class);
        job.setReducerClass(MRVisitsSpendsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MPVisitSpendDTO.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args1[0]));

        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        SequenceFileOutputFormat.setOutputPath(job, new Path(args1[1]));
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        boolean result = job.waitForCompletion(true);

        System.out.println("Browsers :");
        for (Counter counter : job.getCounters().getGroup(Browser.class.getCanonicalName())) {
            System.out.println(" - " + counter.getDisplayName() + ": " + counter.getValue());
        }

        System.exit(result ? 0 : 1);
    }

    public static class MRVisitsSpendsMapper extends Mapper<Object, Text, Text, MPVisitSpendDTO> {

        private static final Pattern IP_PATTERN = Pattern.compile(MRVisitsSpendsMapper.IP_REGEXP);
        private static final String IP_REGEXP = "\\s\\d+\\.\\d+\\.\\d+\\.(\\d+|\\*)\\s";

        private static final Pattern USER_AGENT_PATTERN = Pattern.compile(MRVisitsSpendsMapper.USER_AGENT_REGEXP);
        private static final String USER_AGENT_REGEXP = "[a-zA-Z0-9]+\\s[0-9]+\\s[a-zA-Z0-9]+\\s(.*)";

        private Text ipAddr = new Text();
        private MPVisitSpendDTO mpVisitSpendDTO = new MPVisitSpendDTO();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] params = line.split("\\s+");

            Matcher ipMatcher = IP_PATTERN.matcher(line);
            if (ipMatcher.find()) {
                String ip = ipMatcher.group().trim();
                ipAddr.set(ip);

                Integer spends = Integer.parseInt(params[params.length - 4]);

                mpVisitSpendDTO.setSpends(spends);
                mpVisitSpendDTO.setVisits(1);
                context.write(ipAddr, mpVisitSpendDTO);

                String userParams = line.split(ip)[0].trim();
                Matcher m2 = USER_AGENT_PATTERN.matcher(userParams);
                if (m2.find()) {
                    String userAgent = m2.group(1);
                    UserAgent ua = new UserAgent(userAgent);
                    context.getCounter(ua.getBrowser()).increment(1);
                }
            }
        }
    }


    public static class MRVisitsSpendsReducer extends Reducer<Text, MPVisitSpendDTO, Text, MPVisitSpendDTO> {
        private MPVisitSpendDTO mpVisitSpendDTO = new MPVisitSpendDTO();

        public void reduce(Text key, Iterable<MPVisitSpendDTO> items, Context context)  {
            int visits = 0;
            int price = 0;

            for (MPVisitSpendDTO item : items) {
                visits += item.getVisits();
                price += item.getSpends();
            }

            mpVisitSpendDTO.setVisits(visits);
            mpVisitSpendDTO.setSpends(price);

            try {
                context.write(key, mpVisitSpendDTO);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }

    }
}