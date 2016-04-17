package main.java.frequencyAnalyzer;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;

import main.java.analyzer.Analyze;
import main.java.analyzer.AnalyzeJob;
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
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.json.JSONObject;

/**
 * Created by rene on 17-4-16.
 */
    public class FrequencyJob extends Configured implements Tool {

        @Override
        public int run(String[] arg0) throws Exception {
            Job job = new Job(getConf());

            String separator = "=";

            final Configuration conf = job.getConfiguration();
            conf.set("mapred.textoutputformat.separator", separator);
            conf.set("mapreduce.textoutputformat.separator", separator);
            conf.set("mapreduce.output.textoutputformat.separator", separator);
            conf.set("mapreduce.output.key.field.separator", separator);
            conf.set("mapred.textoutputformat.separatorText", separator);

            job.setJarByClass(getClass());
            job.setJobName(getClass().getSimpleName());

            FileInputFormat.addInputPath(job, new Path("/home/rene/input"));
            FileOutputFormat.setOutputPath(job, new Path("/home/rene/output"));

            job.setMapperClass(FrequencyMapper.class);
            job.setReducerClass(FrequencyReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            if (job.waitForCompletion(true) == true) {
                return 0;
            } else {
                return 1;
            }
        }


    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File("/home/rene/output"));
        FrequencyJob job = new FrequencyJob();
        int rc = ToolRunner.run(job, args);

        JSONObject data = new JSONObject();

        Properties props = new Properties();
        props.load(new FileInputStream(new File("/home/rene/output/part-r-00000")));

        HashMap<String, JSONObject> map = new HashMap<>();
        for (Entry<Object, Object> e : props.entrySet()) {

            String[] keyValue = e.getKey().toString().split("_");
            if (keyValue[0].equals( "-")){
                keyValue[0] = " ";
            }

            try{
                if(map.containsKey(keyValue[0])){
                    JSONObject temp = map.get(keyValue[0]);
                    temp.put(keyValue[1], e.getValue().toString());
                    map.put(keyValue[0], temp);
                }else{
                    JSONObject temp = new JSONObject();
                    temp.put(keyValue[1], e.getValue().toString());
                    map.put(keyValue[0], temp);
                }
            }catch (ArrayIndexOutOfBoundsException ex){
                if(map.containsKey(keyValue[0])){
                    JSONObject temp = map.get(keyValue[0]);
                    temp.put(" ", e.getValue().toString());
                    map.put(keyValue[0], temp);
                }else{
                    JSONObject temp = new JSONObject();
                    temp.put(" ", e.getValue().toString());
                    map.put(keyValue[0], temp);
                }

            }
        }

        for(Entry<String, JSONObject> entry: map.entrySet()){
            data.put(entry.getKey(), entry.getValue());
        }

        FileWriter jsonOutput = new FileWriter(new File("/home/rene/output/part-r-00000.json"));
        jsonOutput.write(data.toString());
        jsonOutput.close();

        AnalyzeJob job2 = new AnalyzeJob();
        rc = ToolRunner.run(job2, args);

        System.exit(rc);

    }

}
