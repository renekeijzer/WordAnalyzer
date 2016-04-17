package main.java.frequencyAnalyzer;

/**
 * Created by rene on 17-4-16.
 */
        import java.io.IOException;


   import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Mapper;

public class FrequencyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String input = value.toString().replaceAll("[^\\w\\s]", "");
        String[] line = input.split(" ");
        for (String word : line) {

            for (int i = 0; i < word.length(); i++) {
                if(i == 0){
                    ctx.write(new Text(("-" + "_" + word.charAt(i)).toUpperCase()), new LongWritable(1));
                }
                try {
                    if (Character.isLetter(word.charAt(i))) {
                        String k = word.charAt(i) + "_" + word.charAt(i + 1);
                        ctx.write(new Text(k.toUpperCase()), new LongWritable(1));
                    }
                } catch (StringIndexOutOfBoundsException e) {
                        String k = word.charAt(i) + "_" + " ";
                        ctx.write(new Text(k.toUpperCase()), new LongWritable(1));
                }
            }
        }

    }

}

