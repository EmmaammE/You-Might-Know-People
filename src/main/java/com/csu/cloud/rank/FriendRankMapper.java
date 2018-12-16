package com.csu.cloud.rank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendRankMapper extends Mapper<LongWritable, Text,LongWritable,FriendRankWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] inputRow = value.toString().split("\\s");
        if (inputRow.length == 1) {
//            have no friends
            return;
        }

        Long fromUser = Long.parseLong(inputRow[0]);
        String[] friends = inputRow[1].split(",");
        Integer sum = friends.length;
        for (int i=0;i<friends.length;i++) {
            Long friend = Long.parseLong(friends[i]);
            context.write(new LongWritable(Long.parseLong(inputRow[0])),new FriendRankWritable(friend,-1L,-1L));
        }


        for (int i=0;i<sum;i++) {
            for (int j=i+1;j<sum;j++) {

                context.write(new LongWritable(Long.parseLong(friends[i])),
                        new FriendRankWritable(Long.parseLong(friends[j]),fromUser,sum.longValue())
                );

                context.write(new LongWritable(Long.parseLong(friends[j])),
                        new FriendRankWritable(Long.parseLong(friends[i]),fromUser,sum.longValue())
                );
            }
        }
    }
}
