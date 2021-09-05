package com.finalproject;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

    public class MapperClass extends Mapper<LongWritable, Text, IntWritable, Text> {

        public void map(LongWritable key, Text value, Context context){

            String[] row = value.toString().split("\\t");
            String productId = row[3].trim();
            int count = Integer.parseInt(row[9].trim());
            try{
                Text id = new Text(productId);
                IntWritable prodRating = new IntWritable(count);
                context.write(prodRating, id);

            }catch(Exception e){
                System.out.println("Something went wrong in Mapper Task: ");
                e.printStackTrace();
            }
        }
    }
