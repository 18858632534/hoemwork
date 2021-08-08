package com.shun.lagou.mr.module_1;

import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner {
    @Override
    public int getPartition(Object o, Object o2, int i) {
        return 0;
    }
}
