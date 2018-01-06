package com.jackerwang.secondarySort.groupAndSort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {
    private Long first;
    private Long second;

    public Pair() {
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        first = input.readLong();
        second = input.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(first);
        out.writeLong(second);
    }

    @Override
    public int compareTo(Pair o) {
        if (first.compareTo(o.getFirst()) == 0) {
            return -1 * second.compareTo(o.getSecond());
        } else {
            return -1 * first.compareTo(o.getFirst());
        }
    }

    public Long getFirst() {
        return first;
    }

    public void setFirst(Long first) {
        this.first = first;
    }

    public Long getSecond() {
        return second;
    }

    public void setSecond(Long second) {
        this.second = second;
    }

}
