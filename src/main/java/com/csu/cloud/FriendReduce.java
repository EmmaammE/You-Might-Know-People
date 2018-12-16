package com.csu.cloud;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class FriendReduce extends Reducer<LongWritable,FriendWritable,LongWritable, Text> {

    @Override
    public void reduce(final LongWritable key, Iterable<FriendWritable> values, Context context) throws IOException, InterruptedException {
        final Map<Long, List<Long>> mutualFriends = new HashMap<Long, List<Long>>();

        for (FriendWritable val:values) {
            Boolean isFriend = (val.mutualFriend == -1);
            //  给用户推荐他
            Long toUser = val.user;
            final Long mutualFriend = val.mutualFriend;

            if (mutualFriends.containsKey(toUser)) {
                if (isFriend) {
                    mutualFriends.put(toUser,null);
                } else if(mutualFriends.get(toUser) != null) {
                    mutualFriends.get(toUser).add(mutualFriend);
                }
            } else {
                if (!isFriend) {
//                    mutualFriends.put(toUser,new ArrayList<Long>(){
//                        {
//                            add(mutualFriend);
//                        }qsdc
//                    });
                    ArrayList<Long> temp = new ArrayList<Long>();
                    temp.add(mutualFriend);
                    mutualFriends.put(toUser,temp);
                } else {
                    mutualFriends.put(toUser,null);
                }
            }
        }

        SortedMap<Long,List<Long>> friendsSortedMap = new TreeMap<Long, List<Long>>(new Comparator<Long>() {

            public int compare(Long o1, Long o2) {
                Integer v1 = mutualFriends.get(o1).size();
                Integer v2 = mutualFriends.get(o2).size();
                if (v1 > v2) {
                    return -1;
                } else if (v1.equals(v2) && o1 < o2) {
                    return -1;
                } else {
                    return 1;
                }
            }
        });

        for (Map.Entry<Long,List<Long>> entry:mutualFriends.entrySet()) {
            if (entry.getValue()!=null) {
                friendsSortedMap.put(entry.getKey(),entry.getValue());
            }
        }

        int i = 0;
        String output = "";
        for (Map.Entry<Long,List<Long>> entry : friendsSortedMap.entrySet()) {
            if (i==0) {
                output = entry.getKey().toString();
            } else if (i<10) {
                output += "," + entry.getKey().toString();
            } else {
                break;
            }

            ++i;
        }

        context.write(key,new Text(output));
    }
}
