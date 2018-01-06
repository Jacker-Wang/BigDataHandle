package com.jackerwang.secondarySort.groupAndSort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Pair, LongWritable, LongWritable, LongWritable> {

    @Override
    public void reduce(Pair k, Iterable<LongWritable> iterable, Reducer<Pair, LongWritable, LongWritable, LongWritable>.Context context)
            throws IOException, InterruptedException {
        System.out.println("1");
        for (LongWritable value : iterable) {
            context.write(new LongWritable(k.getFirst()), value);
        }
    }

}
