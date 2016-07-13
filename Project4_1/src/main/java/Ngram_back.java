import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Zhangwei on 4/5/16.
 */
public class Ngram_back {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static Pattern p = Pattern.compile("([a-z]|((?<=[a-z])'(?=[a-z])))+");

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            ArrayList<String> list = new ArrayList<String>();
            String line = value
                    .toString()
                    .toLowerCase()
                    .replaceAll("(https?|ftp).*?\\s", " ")
                    .replaceAll("</?ref.*?>", " ")
                    .replaceAll("_", " ");
            Matcher m = p.matcher(line);
            while (m.find()) {
                list.add(m.group());
            }
            if (!list.isEmpty()) {
                int length = list.size();
                for (int i = 0; i < length; i++) {
                    word.set(list.get(i));
                    context.write(word, one);
                }
                for (int i = 0; i < length - 1; i++) {
                    word.set(list.get(i) + " " + list.get(i + 1));
                    context.write(word, one);
                }
                for (int i = 0; i < length - 2; i++) {
                    word.set(list.get(i) + " " + list.get(i + 1) + " " + list.get(i + 2));
                    context.write(word, one);
                }
                for (int i = 0; i < length - 3; i++) {
                    word.set(list.get(i) + " " + list.get(i + 1) + " " + list.get(i + 2) + " " + list.get(i + 3));
                    context.write(word, one);
                }
                for (int i = 0; i < length - 4; i++) {
                    word.set(list.get(i) + " " + list.get(i + 1) + " " + list.get(i + 2) + " " + list.get(i + 3) + " " + list.get(i + 4));
                    context.write(word, one);
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "n-gram");
        job.setJarByClass(Ngram_back.class);
        job.setMapperClass(Ngram_back.TokenizerMapper.class);
        job.setCombinerClass(Ngram_back.IntSumReducer.class);
        job.setReducerClass(Ngram_back.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}