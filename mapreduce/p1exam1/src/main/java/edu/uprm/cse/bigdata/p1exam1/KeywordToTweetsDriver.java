package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by ccgarzona on 4/3/17.
 */
public class KeywordToTweetsDriver {
    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(KeywordToTweetsDriver.class);
        job.setJobName("Tweet ids by Word");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(KeywordToTweetsMapper.class);
        job.setReducerClass(KeywordToTweetsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
