package com.shun.lagou.mr.module_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class OrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "OrderDriver");
        job.setJarByClass(OrderDriver.class);
//        3. 指定Mapper/Reducer类
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);
//        4. 指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
//        5. 指定最终输出的kv数据类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);


        //设置使用自定义InputFormat读取数据
//        job.setInputFormatClass(CustomInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("src/main/java/com/shun/lagou/mr/module_1/input")); //指定读取数据的原始路径
//        7. 指定job输出结果路径
        FileOutputFormat.setOutputPath(job, new Path("src/main/java/com/shun/lagou/mr/module_1/output")); //指定结果数据输出路径
//        8. 提交作业
        final boolean flag = job.waitForCompletion(true);
        //jvm退出：正常退出0，非0值则是错误退出
        System.exit(flag ? 0 : 1);
    }
}
