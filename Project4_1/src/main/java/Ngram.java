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
import java.util.regex.Pattern;

/**
 * Created by Zhangwei on 4/5/16.
 */
public class Ngram {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private final static Pattern p = Pattern.compile(
                "(?i)(https?|ftp):\\/\\/[^\\s/$.?#][^\\s]*" +
                        "|</?ref[^>]*>" +
                        "|'(?![a-zA-Z])|(?<![a-zA-Z])'" +
                        "|[^a-zA-Z'\n]");

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = p.matcher(value.toString())
                    .replaceAll(" ")
                    .replaceAll("\\s{2,}", " ")
                    .toLowerCase()
                    .trim();
            if (!line.isEmpty()) {
                String[] words = line.split(" ");
                for (int i = 0; i < words.length; i++) {
                    word.set(words[i]);
                    context.write(word, one);
                }
                for (int i = 0; i < words.length - 1; i++) {
                    word.set(words[i] + " " + words[i + 1]);
                    context.write(word, one);
                }
                for (int i = 0; i < words.length - 2; i++) {
                    word.set(words[i] + " " + words[i + 1] + " " + words[i + 2]);
                    context.write(word, one);
                }
                for (int i = 0; i < words.length - 3; i++) {
                    word.set(words[i] + " " + words[i + 1] + " " + words[i + 2] + " " + words[i + 3]);
                    context.write(word, one);
                }
                for (int i = 0; i < words.length - 4; i++) {
                    word.set(words[i] + " " + words[i + 1] + " " + words[i + 2] + " " + words[i + 3] + " " + words[i + 4]);
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
        job.setJarByClass(Ngram.class);
        job.setMapperClass(Ngram.TokenizerMapper.class);
        job.setCombinerClass(Ngram.IntSumReducer.class);
        job.setReducerClass(Ngram.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}