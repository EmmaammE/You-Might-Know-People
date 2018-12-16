package com.csu.cloud.rank;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FriendRankWritable implements Writable {
    public Long user;
    public Long mutualFriend;
    public Long friendNums;

    public FriendRankWritable(Long user, Long mutualFriend, Long friendNums) {
        this.user = user;
        this.mutualFriend = mutualFriend;
        this.friendNums = friendNums;
    }

    public FriendRankWritable() {
        this(-1L,-1L,-1L);
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(user);
        out.writeLong(mutualFriend);
        out.writeLong(friendNums);
    }

    public void readFields(DataInput in) throws IOException {
        user = in.readLong();
        mutualFriend = in.readLong();
        friendNums = in.readLong();
    }

    @Override
    public String toString() {
        return "user:"+Long.toString(user)+" mutualFriend:"+Long.toString(mutualFriend)+" friendNums:" + Long.toString(friendNums);
    }
}

