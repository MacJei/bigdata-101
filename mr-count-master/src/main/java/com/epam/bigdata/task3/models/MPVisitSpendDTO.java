package com.epam.bigdata.task3.models;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by Aliaksei_Neuski on 9/2/16.
 */
public class MPVisitSpendDTO implements WritableComparable<MPVisitSpendDTO> {

    private int visits;
    private int spends;

    public MPVisitSpendDTO() {
    }

    public MPVisitSpendDTO(int visits, int spends) {
        this.visits = visits;
        this.spends = spends;
    }

    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, visits);
        WritableUtils.writeVInt(out, spends);
    }

    public void readFields(DataInput in) throws IOException {
        visits = WritableUtils.readVInt(in);
        spends = WritableUtils.readVInt(in);
    }

    public int getVisits() {
        return visits;
    }

    public void setVisits(int visits) {
        this.visits = visits;
    }

    public int getSpends() {
        return spends;
    }

    public void setSpends(int spends) {
        this.spends = spends;
    }

    public int compareTo(MPVisitSpendDTO o) {
        if (Integer.compare(visits, o.getVisits()) == 0) {
            return Integer.compare(spends, o.getSpends());
        }
        else {
            return Integer.compare(visits, o.getVisits());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MPVisitSpendDTO that = (MPVisitSpendDTO) o;

        return visits == that.visits && spends == that.spends;

    }

    @Override
    public int hashCode() {
        int result = visits;
        result = 31 * result + spends;
        return result;
    }

    @Override
    public String toString() {
        return "MPVisitSpendDTO{" +
                "visits=" + visits +
                ", spends=" + spends +
                '}';
    }
}