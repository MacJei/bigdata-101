package com.epam.bigdata.task4.mr;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by Aliaksei_Neuski on 9/6/16.
 */
public class MRPinReducer extends Reducer<MRPinCompositeKeyWritable, Text, NullWritable, Text> {

    private long maxSiteImpression;
    private String iPinyouId;

    @Override
    public void reduce(MRPinCompositeKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long localMax = Long.MIN_VALUE;
        for (Text value : values) {
            context.write(NullWritable.get(), value);

            String[] attrs = value.toString().split("\\t");
            if (Insights.SITE_IMPRESSION.toString().equals(attrs[attrs.length - 1])) {
                ++localMax;
            }
        }

        if (maxSiteImpression < localMax && !"null".equals(key.getiPinyouId())) {
            iPinyouId = key.getiPinyouId();
            maxSiteImpression = localMax;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (null != iPinyouId) {
            Counter siteImpressionCounter = context.getCounter(Insights.class.getName(), iPinyouId);
            siteImpressionCounter.setValue(maxSiteImpression);
        }
    }
}
