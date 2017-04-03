package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ccgarzona on 4/3/17.
 */
public class KeywordToTweetsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String ids = "";
        for (Text value : values)
            ids += value + ", ";
        context.write(key, new Text(ids));
    }
}