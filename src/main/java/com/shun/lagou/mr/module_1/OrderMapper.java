package com.shun.lagou.mr.module_1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {

    final IntWritable intWritable = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String val = value.toString();
        intWritable.set(Integer.parseInt(val));
        context.write(intWritable, NullWritable.get());
    }
}
