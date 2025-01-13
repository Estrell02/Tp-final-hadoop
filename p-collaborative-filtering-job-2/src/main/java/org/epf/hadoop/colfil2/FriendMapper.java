package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class FriendMapper extends Mapper<LongWritable, Text, UserPair, IntWritable> {
    private final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /String[] parts = value.toString().split("\t");
        String user = parts[0];
        String[] relations = parts[1].split(",");
        
        // Compare each pair of relations and emit them as keys
        for (int i = 0; i < relations.length; i++) {
            for (int j = i + 1; j < relations.length; j++) {
                // Sort users in lexicographical order to avoid duplicates
                UserPair pair = new UserPair(relations[i], relations[j]);
                context.write(pair, one);
            }
        }
    }
}
