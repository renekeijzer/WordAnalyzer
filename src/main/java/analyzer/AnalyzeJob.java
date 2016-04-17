package main.java.analyzer;

import main.java.frequencyAnalyzer.FrequencyMapper;
import main.java.frequencyAnalyzer.FrequencyReducer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.File;

/**
 * Created by rene on 17-4-16.
 */
public class AnalyzeJob  extends Configured implements Tool {
    @Override
    public int run(String[] arg0) throws Exception {
        Job job = new Job(getConf());

        String separator = "=";
        FileUtils.deleteDirectory(new File("/home/rene/outputshuf"));
        final Configuration conf = job.getConfiguration();
        conf.set("mapred.textoutputformat.separator", separator);
        conf.set("mapreduce.textoutputformat.separator", separator);
        conf.set("mapreduce.output.textoutputformat.separator", separator);
        conf.set("mapreduce.output.key.field.separator", separator);
        conf.set("mapred.textoutputformat.separatorText", separator);

        job.setJarByClass(getClass());
        job.setJobName(getClass().getSimpleName());

        FileInputFormat.addInputPath(job, new Path("/home/rene/bigshuf"));
        FileOutputFormat.setOutputPath(job, new Path("/home/rene/outputshuf"));

        job.setMapperClass(AnalyzeMapper.class);
        job.setReducerClass(AnalyzeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        if (job.waitForCompletion(true) == true) {
            return 0;
        } else {
            return 1;
        }
    }
}
