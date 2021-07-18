package com.jike.bd.wordcount;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text,Flowbean,Text, Flowbean> {

    @Override
    protected void reduce(Text key, Iterable<Flowbean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0 ;
        long sumDownFlow = 0;

        for (Flowbean value:values) {
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }

        Flowbean flowbean = new Flowbean(sumUpFlow, sumDownFlow);
        context.write(key,flowbean);
    }
}
