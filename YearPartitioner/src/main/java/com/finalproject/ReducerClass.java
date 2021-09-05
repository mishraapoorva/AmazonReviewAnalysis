package com.finalproject;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class ReducerClass extends Reducer<Text, Text, Text, NullWritable> {


    protected void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException{
        for(Text t: values){

            context.write(t, NullWritable.get());
        }
    }
}
