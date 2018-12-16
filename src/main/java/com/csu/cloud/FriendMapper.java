package com.csu.cloud;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendMapper extends Mapper<LongWritable,Text,LongWritable,FriendWritable> {


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] inputRow = value.toString().split("\\s");
        if (inputRow.length == 1) {
//            have no friends
            return;
        }

        Long fromUser = Long.parseLong(inputRow[0]);
        String[] friends = inputRow[1].split(",");
        for (int i=0;i<friends.length;i++) {
            Long friend = Long.parseLong(friends[i]);
            context.write(new LongWritable(Long.parseLong(inputRow[0])),new FriendWritable(friend,-1L));
        }


        for (int i=0;i<friends.length;i++) {
            for (int j=i+1;j<friends.length;j++) {

                context.write(new LongWritable(Long.parseLong(friends[i])),
                        new FriendWritable(Long.parseLong(friends[j]),fromUser)
                );

                context.write(new LongWritable(Long.parseLong(friends[j])),
                        new FriendWritable(Long.parseLong(friends[i]),fromUser)
                );
            }
        }
    }
}