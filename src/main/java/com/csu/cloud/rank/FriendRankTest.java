package com.csu.cloud.rank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FriendRankTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "E:/winutil/");
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration,"People You Might Know");
        job.setJarByClass(FriendRankTest.class);
        job.setMapperClass(FriendRankMapper.class);
        job.setReducerClass(FriendRankReduce.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(FriendRankWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
