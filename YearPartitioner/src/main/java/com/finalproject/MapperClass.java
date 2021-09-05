package com.finalproject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

    private Text inputRec = new Text();
    private Text year = new Text();

    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException{

        if(key.get()==0){
            return;
        }

        String[] line = value.toString().split("\\t");
        String[] yearPart = line[14].split("-");
        String yearVal = yearPart[2].trim();

        year.set(yearVal);
        inputRec.set(value);

        context.write(year, inputRec);
    }
}
