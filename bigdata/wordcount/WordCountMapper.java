package com.jike.bd.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text,Text,Flowbean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");
        int len = words.length;
        String number = words[1];
        Long upFlow = Long.parseLong(words[len - 3]);
        Long downFlow = Long.parseLong(words[len - 2]);
        context.write(new Text(number),new Flowbean(upFlow,downFlow));
    }
}
