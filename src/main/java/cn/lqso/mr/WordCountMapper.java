package cn.lqso.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author luojie
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text word = new Text();
    private LongWritable count = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        for (String s: split) {
            word.set(s);
            context.write(word, count);
        }
    }
}
