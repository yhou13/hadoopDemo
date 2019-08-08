package com.hadoop.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
*
* KEYIN:Map任务读数据的KEY类型，offset,是每行数据起始位置的偏移量 Long
* VALUEIN:Map任务读数据的value类型,其实就是一行行的字符串，String
*
* hello world welcome
* hello welcome
*
* KEYOUT:map 方法自定义实现输出的key的类型 ,String
* VALUEOUT:map方法自定义实现输出的value的类型,Integer
*
*
* 词频统计：相同单词的次数（word,1）
*
* Long,String,String,Integer 是JAVA里面的数据类型
*
* Hadhoop自定义类型：序列化和反序列化
* LongWritavble,text，text， IntWritable
* LongWritavble:偏移量
* */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //把Value对应的行数据按照指定的分隔符拆开
        String[] words=value.toString().toLowerCase().split(",");
        for(String word: words){
            //(hello,1)  (world,1)
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
