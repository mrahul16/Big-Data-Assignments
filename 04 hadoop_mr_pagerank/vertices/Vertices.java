package vertices;

import axmultiply.AXMultiply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import vectorcreate.VectorCreate;

import java.io.IOException;

public class Vertices extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

        Job job = Job.getInstance(conf);
        job.setJarByClass(Vertices.class);
        job.setJobName("Rank URL");

        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, EigenMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, VertexMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setReducerClass(VertexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static class EigenMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] comps = value.toString().split("\t");

            if(comps.length == 2) {
                context.write(new Text(comps[0]),
                        new Text("E" + "\t" + comps[1]));
            }
        }
    }

    public static class VertexMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] comps = value.toString().split(" ");

            if(comps.length == 2) {
                context.write(new Text(comps[0]),
                        new Text("V" + "\t" + comps[1]));
            }
        }
    }

    public static class VertexReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            String url = "", rank = "unknown";

            for(Text val: values) {
                String[] comps = val.toString().split("\t");
                if(comps.length == 2) {
                    if(comps[0].equals("E")) {
                        rank = comps[1];
                    }
                    else if(comps[0].equals("V")) {
                        url = comps[1];
                    }
                }
            }

            context.write(new Text(rank), new Text(url));
        }
    }

    public static void main(String[] args) throws Exception {

        ToolRunner.run(new Configuration(), new Vertices(), args);
    }
}
