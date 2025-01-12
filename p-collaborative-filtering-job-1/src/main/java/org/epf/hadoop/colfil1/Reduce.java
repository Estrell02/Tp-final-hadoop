package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        // liste d'amis séparés par des virgules
        for (Text val : values) {
            if (sb.length() > 0) sb.append(",");
            sb.append(val.toString());
        }
        
        context.write(key, new Text(sb.toString()));
    }
}