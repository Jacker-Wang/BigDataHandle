package com.jackerwang.secondarySort;

import org.apache.hadoop.util.ToolRunner;

import com.jackerwang.secondarySort.groupAndSort.MySortJob;

public class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("start");
        ToolRunner.run(new MySortJob(), args);
        System.out.println("succeed");
    }

}
