package vectorcreate;

import matrixcreate.MatrixCreate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class VectorCreate {

    public static class VectorMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] comps = value.toString().split("\t");

            if(comps.length == 3) {
                context.write(new Text(comps[0]),
                        new Text("1"));
            }
        }
    }

    public static class VectorReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

//            int len = 0;
//            ArrayList<Long> dest = new ArrayList<>();
//            for (LongWritable val : values) {
//                dest.add(val.get());
//                len += 1;
//            }
//
//            double imp = ((double) 1) / len;
//            for(Long l: dest) {
//                context.write(new LongWritable(l),
//                        new Text(String.valueOf(key.get()) + "\t" + String.valueOf(imp)));
//            }
            Text t = null;
            for(Text v: values) {
                t = v;
            }
            context.write(key, t);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        Job job = Job.getInstance(conf, "vector create");
        job.setJarByClass(VectorCreate.class);
        job.setMapperClass(VectorMapper.class);
        job.setReducerClass(VectorReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
