package com.heyu.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 四个泛型分别表示输入key、输入value、输出
 */
public class WordCountMap extends Mapper<LongWritable,Text,Text,IntWritable> {

    @Override
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String strs[] = line.split("");
        for(String str:strs){
            context.write(new Text(str),new IntWritable(1));
        }

    }
}
