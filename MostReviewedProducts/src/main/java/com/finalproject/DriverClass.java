package com.finalproject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

// Find the topN reviewed products sorted by count
// Here, secondary sorting technique is used with implementation of comparator class

    public class DriverClass {

        public static void main(String[] args) throws IOException {

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            try {
                Job topNProductsJob = Job.getInstance(conf, "Top N Rated Products");
                topNProductsJob.setJarByClass(DriverClass.class);

                int N = 10;
                topNProductsJob.getConfiguration().setInt("N", N);
                topNProductsJob.setInputFormatClass(TextInputFormat.class);
                topNProductsJob.setOutputFormatClass(TextOutputFormat.class);

                topNProductsJob.setMapperClass(MapperClass.class);
                topNProductsJob.setSortComparatorClass(CountComparator.class);
                topNProductsJob.setReducerClass(ReducerClass.class);
                topNProductsJob.setNumReduceTasks(1);

                topNProductsJob.setMapOutputKeyClass(IntWritable.class);
                topNProductsJob.setMapOutputValueClass(Text.class);
                topNProductsJob.setOutputKeyClass(IntWritable.class);
                topNProductsJob.setOutputValueClass(Text.class);

                FileInputFormat.setInputPaths(topNProductsJob, new Path(args[0]));  // Output file path of totalProducts - MR chaining
                FileOutputFormat.setOutputPath(topNProductsJob, new Path(args[1]));
                if (fs.exists(new Path(args[1]))) {
                    fs.delete(new Path(args[1]), true);
                }

                topNProductsJob.waitForCompletion(true);

            } catch (Exception e) {
                System.out.println("Something went wrong in main class: ");
                e.printStackTrace();

            }
        }
    }
