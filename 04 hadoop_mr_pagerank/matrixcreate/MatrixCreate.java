package matrixcreate;

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
import java.util.StringTokenizer;

public class MatrixCreate {

    public static class DestSrcMapper
            extends Mapper<Object, Text, LongWritable, LongWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] comps = value.toString().split(" ");

            if(comps.length == 2) {
                context.write(new LongWritable(Long.parseLong(comps[0])),
                        new LongWritable(Long.parseLong(comps[1])));
            }
        }
    }

    public static class ImportanceReducer
            extends Reducer<LongWritable, LongWritable, LongWritable, Text> {

        public void reduce(LongWritable key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            int sum = 0;
            int len = 0;
            ArrayList<Long> dest = new ArrayList<>();
            for (LongWritable val : values) {
                dest.add(val.get());
                len += 1;
            }

            double imp = ((double) 1) / len;
            if(imp * 1000 >= 1) {
                for (Long l : dest) {
//                context.write(new LongWritable(l),
//                        new Text(String.valueOf(key.get()) + "\t" + String.valueOf(imp)));
                    context.write(new LongWritable(l),
                            new Text(String.valueOf(key.get()) + "\t" + String.valueOf(imp)));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        Job job = Job.getInstance(conf, "matrix create");
        job.setJarByClass(MatrixCreate.class);
        job.setMapperClass(DestSrcMapper.class);
        job.setReducerClass(ImportanceReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
//        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}