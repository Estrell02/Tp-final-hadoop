package org.epf.hadoop.colfil1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ColFilJob1 {
    public static void main(String[]args){
        try {
        
        if (args.length != 2) {
            System.err.println("Usage: Job1 <input path> <output path>");
            System.exit(-1);
        }

        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, " Job 1");

        
        job.setJarByClass(ColFilJob1.class);

        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        
        job.setInputFormatClass(RelationshipInputFormat.class);

        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

       
        job.setNumReduceTasks(2);

        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
       } catch (ClassNotFoundException e) {
        System.err.println("Classe introuvable : " + e.getMessage());
        e.printStackTrace();
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        System.err.println("Thread interrompu : " + e.getMessage());
    } catch (IOException e) {
        e.printStackTrace();
    }
    }
}