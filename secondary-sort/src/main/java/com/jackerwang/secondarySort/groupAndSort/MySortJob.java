package com.jackerwang.secondarySort.groupAndSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class MySortJob extends Configured implements Tool {

    @Override
    public int run(String[] arg0) throws Exception {
        // 调用基类Configured的getConf获取环境变量实例
        Configuration conf = getConf();

        @SuppressWarnings("deprecation")
        Job job = new Job(conf);
        job.setJarByClass(MySortJob.class);
        job.setJobName("groupAndSort");

        // Path input = new Path("hdfs://h3/user/odp/wp/word.txt");
        // Path output = new Path("hdfs://h3/user/odp/wp/wordOutPut.txt");

        Path input = new Path("hdfs://localhost:9000/word.txt");
        Path output = new Path("hdfs://localhost:9000/wordOutPut");

        // Mapper类和Reduce类
        job.setMapperClass(com.jackerwang.secondarySort.groupAndSort.MyMapper.class);
        job.setReducerClass(com.jackerwang.secondarySort.groupAndSort.MyReducer.class);

        // 设置分组比较器，将第一个数字相同的分到一个组
        job.setGroupingComparatorClass(com.jackerwang.secondarySort.groupAndSort.MyGroupComparator.class);

        // 设置输入输出路径
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // 设置map和reduce的键值类型
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(LongWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}