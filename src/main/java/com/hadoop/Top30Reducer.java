package com.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class Top30Reducer  extends Reducer< Text, IntWritable, Text, LongWritable> {
    Text text = new Text();
    TreeMap<Integer,String > map = new TreeMap<Integer,String>();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> value, Context context)
            throws IOException, InterruptedException {
        int sum=0;//key出现次数
        for (IntWritable ltext : value) {
            sum+=ltext.get();
        }
        map.put(sum,key.toString());
        //取前30条数据
        if(map.size()>30){
            map.remove(map.firstKey());
        }
    }
    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        for(Integer count:map.keySet()){
            context.write(new Text(map.get(count)), new LongWritable(count));
        }
    }
}

