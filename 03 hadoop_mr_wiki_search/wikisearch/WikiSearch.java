package wikisearch;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WikiSearch {

    public static class SearchMapper
            extends Mapper<Object, Text, Text, NullWritable>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            Configuration configuration = context.getConfiguration();
            String keyword = configuration.get("keyword");
            String sv = value.toString();
            String[] lines = sv.split("\n");

            String url = "Uninitialized";

            for(String line: lines) {
                String[] lcomps = line.split("\t");
                String first = lcomps[0];
                if(first.length() > 0) {
                    if(first.toUpperCase().equals(first) && lcomps.length > 1) {
                        if (lcomps[1].equalsIgnoreCase(keyword)) {
                            context.write(new Text(url), NullWritable.get());
                        }
                    }
                    else {
                        url = first;
                    }
                }
            }
        }
    }

    public static class SearchReducer
            extends Reducer<Text, NullWritable, Text, NullWritable> {

        public void reduce(Text key, NullWritable values,
                           Context context
        ) throws IOException, InterruptedException {
            context.write(new Text(values.toString()), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        String regex = "URL.*";
        Configuration conf = new Configuration(true);
        conf.set("textinputformat.record.delimiter","URL\t");
        conf.set("keyword", args[2]);
        Job job = Job.getInstance(conf, "keyword search");
        job.setJarByClass(WikiSearch.class);
        job.setMapperClass(SearchMapper.class);
        job.setCombinerClass(SearchReducer.class);
        job.setReducerClass(SearchReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}