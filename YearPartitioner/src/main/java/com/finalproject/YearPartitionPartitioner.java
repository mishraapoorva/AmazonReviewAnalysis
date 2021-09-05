package com.finalproject;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearPartitionPartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numPartitions){
        int n=1;
        if(numPartitions==0){
            return 0;
        }
        else if(key.equals(("99"))){
            return n % numPartitions;
        }
        else if(key.equals(new Text("00"))){
            return 2 % numPartitions;
        }
        else if(key.equals(new Text("01"))){
            return 3 % numPartitions ;
        }
        else if(key.equals(new Text("02"))){
            return 4 % numPartitions;
        }
        else if(key.equals(new Text("03"))){
            return 5 % numPartitions;
        }
        else if(key.equals(new Text("04"))){
            return 6 % numPartitions;
        }
        else if(key.equals(new Text("05"))){
            return 7 % numPartitions;
        }
        else if(key.equals(new Text("06"))){
            return 8 % numPartitions;
        }
        else if(key.equals(new Text("07"))){
            return 9 % numPartitions;
        }
        else if(key.equals(new Text("08"))){
            return 10 % numPartitions;
        }
        else if (key.equals(new Text("09"))){
            return 11 % numPartitions;
        }
        else if (key.equals(new Text("10"))){
            return 12 % numPartitions;
        }
        else if (key.equals(new Text("11"))){
            return 13 % numPartitions;
        }
        else
        {
            return 14 % numPartitions;
        }
    }
}
