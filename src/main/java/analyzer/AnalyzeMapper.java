package main.java.analyzer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by rene on 17-4-16.
 */
public class AnalyzeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    public void map(LongWritable key, Text value, Mapper.Context ctx) throws IOException, InterruptedException {
        Analyze an = new Analyze();
        String input = value.toString().replaceAll("[^\\w\\s]", "");
        String[] words = input.split(" ");
        double percentage = 0;
        int badCount = 0;
        try {
            for (String word : words){
                    if(an.predict(word) < 7.5){
                        badCount +=1;
                    }
            }
        }catch (Exception ex){

        }
        if (badCount > words.length/2){
            ctx.write(new Text(input), new LongWritable(badCount));
        }


    }
}
