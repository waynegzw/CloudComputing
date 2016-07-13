import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Created by Zhangwei on 4/6/16.
 */
public class LanguageModel {

    private static int T = 2;
    private static int N = 5;
    private static final String TName = "predictor";
    private static final String DNS = "ec2-54-86-141-188.compute-1.amazonaws.com";

    public static class LanguageModelMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text phrase = new Text();
        private Text content = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            if (tokens.length == 2 && Integer.parseInt(tokens[1]) > T) {
                phrase.set(tokens[0]);
                content.set(tokens[1]);
                context.write(phrase, content);
                String[] words = tokens[0].split(" ");
                if (words.length > 1) {
                    String subPhrase = words[0];
                    for (int i = 1; i < words.length - 1; i++) {
                        subPhrase += " " + words[i];
                    }
                    phrase.set(subPhrase);
                    String v = words[words.length - 1] + ":" + tokens[1];
                    content.set(v);
                    context.write(phrase, content);
                }
            }
        }
    }

    public static class LanguageModelReducer
            extends TableReducer<Text, Text, ImmutableBytesWritable> {

        private static byte[] columnFamily = Bytes.toBytes("p");

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int phraseCount = 0;
            TreeSet<String> predictorTree = new TreeSet<String>(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    String[] s1 = o1.split(":");
                    String[] s2 = o2.split(":");
                    if (Integer.compare(Integer.parseInt(s2[1]), Integer.parseInt(s1[1])) == 0) {
                        return s1[0].compareTo(s2[0]);
                    } else {
                        return Integer.compare(Integer.parseInt(s2[1]), Integer.parseInt(s1[1]));
                    }
                }
            });
            for (Text val : values) {
                String[] content = val.toString().split(":");
                if (content.length == 1) {
                    phraseCount = Integer.parseInt(content[0]);
                } else {
                    predictorTree.add(val.toString());
                }
            }
            byte[] rowKey = Bytes.toBytes(key.toString());
            Iterator<String> it = predictorTree.iterator();
            int n = 0;
            while (it.hasNext() && n < N) {
                n++;
                String[] raw = it.next().split(":");
                String word = raw[0];
                int phraseWordCount = Integer.parseInt(raw[1]);
                double prob = ((double) phraseWordCount) / phraseCount;
                Put put = new Put(rowKey);
                put.add(columnFamily, Bytes.toBytes(word), Bytes.toBytes(String.format("%.2f", prob)));
                context.write(new ImmutableBytesWritable(rowKey), put);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        T = Integer.parseInt(args[1]);
        N = Integer.parseInt(args[2]);
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", DNS);
        Job job = Job.getInstance(conf, "LanguageModel");
        job.setJarByClass(LanguageModel.class);
        TableMapReduceUtil.initTableReducerJob(TName, LanguageModelReducer.class, job);
        job.setMapperClass(LanguageModelMapper.class);
        job.setReducerClass(LanguageModelReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
