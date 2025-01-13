package org.epf.hadoop.colfil2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ColFilJob2 {

    public static void main(String[] args) throws Exception {
        try {
            if (args.length != 2) {
                System.err.println("Usage: FriendRecommenderJob <input path> <output path>");
                System.exit(-1);
            }

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Friend Recommender Job");

            job.setJarByClass(ColFilJob2.class);
            job.setMapperClass(FriendMapper.class);
            job.setReducerClass(FriendReducer.class);

            job.setNumReduceTasks(2);

            job.setOutputKeyClass(UserPair.class);  // Corriger ici, utilisez UserPair comme clé
            job.setOutputValueClass(Text.class);    // Vous pouvez garder Text comme valeur

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
