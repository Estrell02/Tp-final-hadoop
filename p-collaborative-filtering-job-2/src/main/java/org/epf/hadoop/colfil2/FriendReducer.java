package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendReducer extends Reducer<UserPair, IntWritable, Text, Text> {
    private final Text result = new Text();

    @Override
   public void reduce(UserPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        
        // Sum all occurrences for the given pair
        for (IntWritable val : values) {
            count += val.get();
        }

        // Emit the result: pair of users and the number of common relations
        context.write(key, new IntWritable(count));
    }
}
