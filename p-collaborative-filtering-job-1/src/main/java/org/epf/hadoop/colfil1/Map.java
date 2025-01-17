package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;


public class Map extends Mapper<LongWritable, Relationship, Text, Text> {
    @Override
    public void map(LongWritable key, Relationship value, Context context) throws IOException, InterruptedException {
        // Emet A->B
        context.write(new Text(value.getId1()), new Text(value.getId2()));
        // Emet B->A
        context.write(new Text(value.getId2()), new Text(value.getId1()));
    }
}
