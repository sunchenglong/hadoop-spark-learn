package com.suncl.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by suncl on 2016/7/23.
 */
public class MaxTemp {
    public static void main(String[] args) throws Exception {
        String dst = "hdfs://centos:9000/home/hadooplearn/intput.txt";
        String dstOut = "hdfs://localhost:9000/hadooplearn/output";
        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        Job job = new Job(hadoopConfig);
        //job.setJarByClass(NewMaxTemperature.class);
        FileInputFormat.addInputPath(job, new Path(dst));
        FileOutputFormat.setOutputPath(job, new Path(dstOut));
        job.setMapperClass(MaxTempMapper.class);
        job.setReducerClass(MaxTempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);
        System.out.println("Finished");
    }
}

class MaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.print("Before Mapper: " + key + ", " + value);
        String line = value.toString();
        String year = line.substring(0, 4);
        int temperature = Integer.parseInt(line.substring(8));
        context.write(new Text(year), new IntWritable(temperature));
        System.out.println("======After Mapper:" + new Text(year) + ", " + new IntWritable(temperature));
    }
}
class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxValue = Integer.MIN_VALUE;
        StringBuffer sb = new StringBuffer();
        for (IntWritable value : values) {
            maxValue = Math.max(maxValue, value.get());
            sb.append(value).append(", ");
        }
        System.out.print("Before Reduce: " + key + ", " + sb.toString());
        context.write(key, new IntWritable(maxValue));
        System.out.println("======After Reduce: " + key + ", " + maxValue);
    }

}
