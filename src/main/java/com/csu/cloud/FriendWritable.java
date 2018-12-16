package com.csu.cloud;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FriendWritable implements Writable {
    public Long user;
    public Long mutualFriend;

    public FriendWritable(Long user,Long mutualFriend) {
        this.user = user;
        this.mutualFriend = mutualFriend;
    }

    public FriendWritable(){
        this(-1L,-1L);
    }
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(user);
        dataOutput.writeLong(mutualFriend);
    }

    public void readFields(DataInput dataInput) throws IOException {
        user = dataInput.readLong();
        mutualFriend = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "User:"+Long.toString(user)+" mutualFriend:"+Long.toString(mutualFriend);
    }
}
