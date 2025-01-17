package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendReducer extends Reducer<UserPair, IntWritable, UserPair, IntWritable> {
    @Override
    protected void reduce(UserPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        boolean directConnection = false;

        
        for (IntWritable value : values) {
            if (value.get() == 0) {
                directConnection = true;
            } else {
                count += value.get();
            }
        }

    
        if (!directConnection && count > 0) {
            if (!key.getFirstUser().equals(key.getSecondUser())) {
                context.write(key, new IntWritable(count));
                           }
            // context.write(key, new IntWritable(count));
        }
    }
}
