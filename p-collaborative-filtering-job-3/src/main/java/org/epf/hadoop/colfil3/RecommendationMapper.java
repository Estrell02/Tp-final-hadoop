package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RecommendationMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\\t");
        if (line.length != 2) return;

        String[] users = line[0].split(",");
        String user1 = users[0];
        String user2 = users[1];
        String score = line[1];

       
        context.write(new Text(user1), new Text(user2 + ":" + score));
        context.write(new Text(user2), new Text(user1 + ":" + score));
    }
}
