package temp;

import matrixcreate.MatrixCreate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class Temp {

    public static class TempMapper
            extends Mapper<Object, Text, DoubleWritable, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] comps = value.toString().split("\t");

            if(comps.length == 2) {
                if(!comps[0].equals("unknown"))
                    context.write(new DoubleWritable(Double.parseDouble(comps[0])),
                        new Text(comps[1]));
            }
        }
    }

    public static class TempReducer
            extends Reducer<DoubleWritable, Text, Text, Text> {

        public void reduce(DoubleWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            double d = key.get();
            for(Text val:values) {
                context.write(new Text(String.valueOf(d)), val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        Job job = Job.getInstance(conf, "temp");
        job.setJarByClass(Temp.class);
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
