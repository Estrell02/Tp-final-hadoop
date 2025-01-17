package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class FriendMapper extends Mapper<Object, Text, UserPair, IntWritable> {
    private final static IntWritable ONE = new IntWritable(1);
    private final static IntWritable ZERO = new IntWritable(0);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
       
        String[] parts = value.toString().split("\\t");
        if (parts.length < 2) return;

        String user = parts[0];
        String[] friends = parts[1].split(",");

        
        for (int i = 0; i < friends.length; i++) {
            for (int j = i + 1; j < friends.length; j++) {
                String friend1 = friends[i];
                String friend2 = friends[j];
                UserPair pair = new UserPair(friend1, friend2);
                context.write(pair, ONE);
            }
        }
        

        // direct relationships avec ZERO
        for (String friend : friends) {
            UserPair directPair = new UserPair(user, friend);
            context.write(directPair, ZERO);
        }
    }
}
