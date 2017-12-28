package axmultiply;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

public class AXMultiply extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        int numIter = 10;
        System.out.println("Running " + numIter + " iterations...");
        FileSystem hdfs = FileSystem.get(new Configuration());
        Path homeDir = hdfs.getHomeDirectory();
        System.out.println(hdfs.getWorkingDirectory());
//        Path tempPath = new Path("/"+ args[3] + "/ax/output/temp");
        Path tempPath = new Path("/" + args[2] + "/temp");
        tempPath = Path.mergePaths(homeDir, tempPath);


        for(int i = 1; i <= numIter; i++) {

            JobControl jobControl = new JobControl("jobChain");
            Configuration conf1 = getConf();
            conf1.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

            Job job1 = Job.getInstance(conf1);
            job1.setJarByClass(AXMultiply.class);
            job1.setJobName("Merge Multiply Ax");

            MultipleInputs.addInputPath(job1, new Path(args[0]),
                    TextInputFormat.class, AlignMatrixMapper.class);

            if(i > 1) {
                MultipleInputs.addInputPath(job1, new Path(args[2] + "/temp" + String.valueOf(i - 1)),
                        TextInputFormat.class, AlignVectorMapper.class);
            }
            else {
                MultipleInputs.addInputPath(job1, new Path(args[1]),
                        TextInputFormat.class, AlignVectorMapper.class);
            }

            FileOutputFormat.setOutputPath(job1, new Path(args[2] + "/temp"));
            
            job1.setReducerClass(AlignReducer1.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            ControlledJob controlledJob1 = new ControlledJob(conf1);
            controlledJob1.setJob(job1);

            jobControl.addJob(controlledJob1);
            Configuration conf2 = getConf();
            conf2.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

            Job job2 = Job.getInstance(conf2);
            job2.setJarByClass(AXMultiply.class);
            job2.setJobName("Row Sum");

            FileInputFormat.setInputPaths(job2, new Path(args[2] + "/temp"));
            if(i < numIter) {
                FileOutputFormat.setOutputPath(job2, new Path(args[2] + "/temp" + String.valueOf(i)));
            }
            else {
                FileOutputFormat.setOutputPath(job2, new Path(args[2] + "/final"));
            }

            job2.setMapperClass(RowValueMapper.class);
            job2.setReducerClass(AlignReducer2.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            ControlledJob controlledJob2 = new ControlledJob(conf2);
            controlledJob2.setJob(job2);

            // make job2 dependent on job1
            controlledJob2.addDependingJob(controlledJob1);
            // add the job to the job control
            jobControl.addJob(controlledJob2);
            Thread jobControlThread = new Thread(jobControl);
            jobControlThread.start();

            while (!jobControl.allFinished()) {
                System.out.println(i + "\t"
                        + new Date().toString() + "\t"
                        + "Waiting: " + jobControl.getWaitingJobList().size() + "\t"
                        + "Ready: " + jobControl.getReadyJobsList().size() + "\t"
                        + "Running: " + jobControl.getRunningJobList().size() + "\t"
                        + "Success: " + jobControl.getSuccessfulJobList().size() + "\t"
                        + "Failed: " + jobControl.getFailedJobList().size());

                try {
                    Thread.sleep(10000);
                } catch (Exception e) {

                }

            }

            if(hdfs.exists(tempPath)) {
                hdfs.delete(tempPath, true);
            }

            Path temp_i = Path.mergePaths(tempPath, new Path(String.valueOf(i - 1)));
            if(hdfs.exists(temp_i)) {
                hdfs.delete(temp_i, true);
            }


            System.out.println("------------------------------------------------------------------------------------");
        }
        System.exit(0);
//        return (job1.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static class AlignVectorMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

//            Configuration configuration = context.getConfiguration();
//            String keyword = configuration.get("keyword");
//            String sv = value.toString();
            String[] comps = value.toString().split("\t");

            if(comps.length == 2) {
//                int r = Integer.parseInt(comps[0]);
                context.write(new Text(comps[0]), new Text(comps[1]));
            }

        }
    }

    public static class AlignMatrixMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] comps = value.toString().split("\t");

            if(comps.length == 3) {
                context.write(new Text(comps[1]),
                        new Text(comps[0] + "\t" + comps[2]));
            }

        }
    }

    public static class AlignReducer1
            extends Reducer<Text, Text, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            double vectorValue = 0;
            ArrayList<String> matrixValues = new ArrayList<>();
            String[] comps;

            for(Text value: values) {
                comps = value.toString().split("\t");
                if(comps.length == 1) {
                    vectorValue += Double.parseDouble(comps[0]);
                }
                else {
                    matrixValues.add(value.toString());
                }

            }
            for(String value: matrixValues) {
                comps = value.split("\t");
                if(comps.length == 2) {
                    context.write(new Text(comps[0]),
                            new DoubleWritable(Double.parseDouble(comps[1]) * vectorValue));
                }
            }
        }
    }

    public static class RowValueMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] comps = value.toString().split("\t");

            if(comps.length == 2) {
                context.write(new Text(comps[0]), new Text(comps[1]));
            }

        }
    }

    public static class AlignReducer2
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            double sum = 0.0;

            for(Text value: values) {
                sum += Double.parseDouble(value.toString());
            }

            context.write(key, new Text(String.valueOf(sum)));
        }
    }

    public static void main(String[] args) throws Exception {

        ToolRunner.run(new Configuration(), new AXMultiply(), args);
    }
}
