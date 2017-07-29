import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
 * Created by Venkatram on 7/29/2017.
 */

public class MaxMinTemperature {



    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
        public static final int MISSING = 9999;
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{

            String line = value.toString();
            if(!(line.length() == 0)){
                String date = line.substring(6,14);
                int tempMax = (int)Float.parseFloat(line.substring(39,45).trim());

                int tempMin = (int)Float.parseFloat(line.substring(47,53).trim());

                if(tempMax > 35.0 && tempMax != MISSING){
                    context.write(new Text("Hot Day " + date), new IntWritable(tempMax));
                }

                if(tempMin < 10.0 && tempMax != MISSING){
                    context.write(new Text("Cold Day " + date), new IntWritable(tempMin));
                }
            }

        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{

            int temp = 0;
            for(IntWritable w: values){
                temp = w.get();
            }
            context.write(key, new IntWritable(temp));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"MaxMinTemperature");
        job.setJarByClass(MaxMinTemperature.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        outputPath.getFileSystem(conf).delete(outputPath,true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


}
