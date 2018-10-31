package com.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Top30Mapper  extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        // 1 获取一行 访问时间\t 用户 ID\t[查询词]\t 该 URL 在返回结果中的排名\t 用户点击的顺序号\t 用户点击的 URL
        String line = value.toString();
        // 2 切割
        String[] fields = line.split("\t");
        // 3 获取 查询词
        String searchWord = fields[2];
        // 输出 <关键词，次数>
        context.write(new Text(searchWord),new IntWritable(1) );
    }
}
