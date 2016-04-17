package main.java.frequencyAnalyzer;

/**
 * Created by rene on 17-4-16.
 */

        import java.io.IOException;

        import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Reducer;

public class FrequencyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    public void reduce(Text key, Iterable<LongWritable> values, Context ctx) throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable l : values) {
            sum += l.get();
        }
        ctx.write(key, new LongWritable(sum));
    }

}
