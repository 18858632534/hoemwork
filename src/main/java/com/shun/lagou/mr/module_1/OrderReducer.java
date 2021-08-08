package com.shun.lagou.mr.module_1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {

    final IntWritable id = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable nullWritable: values){
            id.set(id.get() + 1);
            context.write(id, key);
        }
    }
}
