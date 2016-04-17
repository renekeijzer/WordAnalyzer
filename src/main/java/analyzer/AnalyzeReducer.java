package main.java.analyzer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by rene on 17-4-16.
 */
public class AnalyzeReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    public void reduce(Text key, Iterable<LongWritable> values, Reducer.Context ctx) throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable l : values) {
            sum += l.get();
        }
        if (sum / 100 < 10) {
            ctx.write(key, new LongWritable(sum));
        }
    }
}
