package com.jackerwang.secondarySort.groupAndSort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator implements RawComparator<Pair> {

    @Override
    public int compare(Pair o1, Pair o2) {

        return (int) (o1.getFirst() - o2.getFirst());
    }

    @Override
    public int compare(byte[] b1, int s1, int arg2, byte[] b2, int s2, int arg5) {
        // Long占8个字节
        return WritableComparator.compareBytes(b1, s1, 8, b2, s2, 8);
    }
}
