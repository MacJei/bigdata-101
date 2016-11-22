package com.epam.bigdata.task4.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

/**
 * Created by Aliaksei_Neuski on 9/6/16.
 */
public class MRPinCompositeKeyWritable implements Writable, WritableComparable<MRPinCompositeKeyWritable> {

    private String iPinyouId;
    private long timestamp;

    /*
    Always there should be default constructor
     */
    public MRPinCompositeKeyWritable() {
    }

    public MRPinCompositeKeyWritable(String iPinyouId, long timestamp) {
        this.iPinyouId = iPinyouId;
        this.timestamp = timestamp;
    }

    public void readFields(DataInput dataInput) throws IOException {
        iPinyouId = WritableUtils.readString(dataInput);
        timestamp = WritableUtils.readVLong(dataInput);
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, iPinyouId);
        WritableUtils.writeVLong(dataOutput, timestamp);
    }

    public String getiPinyouId() {
        return iPinyouId;
    }

    public void setiPinyouId(String iPinyouId) {
        this.iPinyouId = iPinyouId;
    }

    public long getTimestapm() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MRPinCompositeKeyWritable that = (MRPinCompositeKeyWritable) o;

        if (timestamp != that.timestamp) return false;
        return iPinyouId != null ? iPinyouId.equals(that.iPinyouId) : that.iPinyouId == null;

    }

    @Override
    public int hashCode() {
        int result = iPinyouId != null ? iPinyouId.hashCode() : 0;
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    public int compareTo(MRPinCompositeKeyWritable objKeyPair) {
        // TODO:
		/*
		 * Note: This code will work as it stands; but when CompositeKeyWritable
		 * is used as key in a map-reduce program, it is de-serialized into an
		 * object for comapareTo() method to be invoked;
		 *
		 * To do: To optimize for speed, implement a raw comparator - will
		 * support comparison of serialized representations
		 */
        int result = iPinyouId.compareTo(objKeyPair.iPinyouId);
        if (0 == result) {
            result = Long.compare(timestamp, objKeyPair.timestamp) ;
        }
        return result;
    }

    @Override
    public String toString() {
        return (new StringBuilder().append(iPinyouId).append("\t").append(timestamp)).toString();
    }
}
