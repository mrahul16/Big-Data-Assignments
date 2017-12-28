package wikicount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WikiCount {

    public static class CountMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            Configuration configuration = context.getConfiguration();
            String keyword = configuration.get("keyword");
            String sv = value.toString();
            String[] lines = sv.split("\n");

            int count = 0;

            for(String line: lines) {
                String[] lcomps = line.split("\t");
                String first = lcomps[0];
                if(first.length() > 0) {
                    if(first.toUpperCase().equals(first) && lcomps.length > 1) {
                        if (lcomps[1].equalsIgnoreCase(keyword)) {
                            context.write(new Text(lcomps[1]), new IntWritable(1));
//                            count++;
                        }
                    }
                }
            }

//            context.write(new IntWritable(count), NullWritable.get());
        }
    }

    public static class CountReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            Configuration configuration = context.getConfiguration();
            String keyword = configuration.get("keyword");
            int sum = 0;
            for(IntWritable i: values) {

                sum += i.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        String regex = "URL.*";
        Configuration conf = new Configuration(true);
        conf.set("textinputformat.record.delimiter","URL\t");
        conf.set("keyword", args[2]);
        Job job = Job.getInstance(conf, "keyword count");
        job.setJarByClass(WikiCount.class);

        job.setMapperClass(WikiCount.CountMapper.class);
//        job.setCombinerClass(WikiCount.CountReducer.class);
        job.setReducerClass(WikiCount.CountReducer.class);

//        job.setMapOutputKeyClass(IntWritable.class);
//        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(2);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
