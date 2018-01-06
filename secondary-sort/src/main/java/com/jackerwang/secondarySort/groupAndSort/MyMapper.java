package com.jackerwang.secondarySort.groupAndSort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Pair, LongWritable> {

    @Override
    public void map(LongWritable arg0, Text value, Mapper<LongWritable, Text, Pair, LongWritable>.Context context) throws IOException, InterruptedException {
        String[] spilted = value.toString().split("\t");
        Long firstNum = Long.parseLong(spilted[0]);
        Long secondNum = Long.parseLong(spilted[1]);

        Pair pair = new Pair();
        pair.setFirst(firstNum);
        pair.setSecond(secondNum);

        context.write(pair, new LongWritable(secondNum));
    };
}
