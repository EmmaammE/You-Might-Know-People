package com.csu.cloud.rank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class FriendRankReduce extends Reducer<LongWritable,FriendRankWritable,LongWritable, Text> {

    public Double getRankOfFriend(List<List<Long>> friends){
        Double rank = 0.0;
        for (List<Long> val:friends) {
            rank += 1/Math.log(val.get(1));
        }
        return rank;
    }

    @Override
    public void reduce(LongWritable key, Iterable<FriendRankWritable> values, Context context) throws IOException, InterruptedException {
        final Map<Long, List<List<Long>>> mutualFriends = new HashMap<Long, List<List<Long>>>();

        for (FriendRankWritable val:values) {
            Boolean isFriend = (val.mutualFriend == -1);
            //  给用户推荐他
            Long toUser = val.user;
//            final Long mutualFriend = val.mutualFriend;
            List<Long> friend_num = new ArrayList<Long>();
            friend_num.add(val.mutualFriend);
            friend_num.add(val.friendNums);

            if (mutualFriends.containsKey(toUser)) {
                if (isFriend) {
                    mutualFriends.put(toUser,null);
                } else if(mutualFriends.get(toUser) != null) {
                    mutualFriends.get(toUser).add(friend_num);
                }
            } else {
                if (!isFriend) {
                    ArrayList<List<Long>> temp = new ArrayList<List<Long>>();
                    temp.add(friend_num);
                    mutualFriends.put(toUser,temp);
                } else {
                    mutualFriends.put(toUser,null);
                }
            }
        }

        SortedMap<Long,List<List<Long>>> friendsRankSortedMap = new TreeMap<Long, List<List<Long>>>(new Comparator<Long>() {

            public int compare(Long o1, Long o2) {

                Double v1 = getRankOfFriend(mutualFriends.get(o1));
                Double v2 = getRankOfFriend(mutualFriends.get(o2));
                if ( v1.compareTo(v2) > 0 ) {
                    return -1;
                } else if (v1.compareTo(v2)==0 && o1 < o2) {
                    return -1;
                } else {
                    return 1;
                }
            }
        });

        for (Map.Entry<Long,List<List<Long>>> entry:mutualFriends.entrySet()) {
            if (entry.getValue()!=null) {
                friendsRankSortedMap.put(entry.getKey(),entry.getValue());
            }
        }

        int i = 0;
        String output = "";
        for (Map.Entry<Long,List<List<Long>>> entry : friendsRankSortedMap.entrySet()) {
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
