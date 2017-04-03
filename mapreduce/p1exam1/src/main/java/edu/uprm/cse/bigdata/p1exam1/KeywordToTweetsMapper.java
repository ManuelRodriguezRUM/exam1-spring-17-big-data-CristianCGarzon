package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

/**
 * Created by ccgarzona on 4/3/17.
 */
public class KeywordToTweetsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String originalTweet = value.toString();

        try {
            Status tweet = TwitterObjectFactory.createStatus(originalTweet);
            if(tweet.getText().toUpperCase().contains("MAGA")){
                context.write((new Text("MAGA")), new Text(Long.toString(tweet.getId())));
            }
            if(tweet.getText().toUpperCase().contains("DICTATOR")){
                context.write((new Text("DICTATOR")), new Text(Long.toString(tweet.getId())));
            }
            if(tweet.getText().toUpperCase().contains("IMPEACH")){
                context.write((new Text("IMPEACH")), new Text(Long.toString(tweet.getId())));
            }
            if(tweet.getText().toUpperCase().contains("DRAIN")){
                context.write((new Text("DRAIN")), new Text(Long.toString(tweet.getId())));
            }
            if(tweet.getText().toUpperCase().contains("SWAMP")){
                context.write((new Text("SWAMP")), new Text(Long.toString(tweet.getId())));
            }
            if(tweet.getText().toUpperCase().contains("CHANGE")){
                context.write((new Text("CHANGE")), new Text(Long.toString(tweet.getId())));
            }
        } catch (TwitterException e) {
            e.printStackTrace();
        }
    }
}
