package com.epam.bigdata.task4.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * Created by Aliaksei_Neuski on 9/6/16.
 */
public class MRPinCompKeySortComparator extends WritableComparator {

    protected MRPinCompKeySortComparator() {
        super(MRPinCompositeKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        MRPinCompositeKeyWritable key1 = (MRPinCompositeKeyWritable) w1;
        MRPinCompositeKeyWritable key2 = (MRPinCompositeKeyWritable) w2;

        int cmpResult = key1.getiPinyouId().compareTo(key2.getiPinyouId());
        if (cmpResult == 0) { // the same
            return Long.compare(key1.getTimestapm(), key2.getTimestapm());
        }
        return cmpResult;
    }
}
