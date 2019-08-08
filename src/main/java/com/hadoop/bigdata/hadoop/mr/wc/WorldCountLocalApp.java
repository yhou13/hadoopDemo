package com.hadoop.bigdata.hadoop.mr.wc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/*
*
* 使用本地文件进行统计，然后统计结果输出到本地路径
* */
public class WorldCountLocalApp {
    public static void main(String[] args) throws Exception{
        Configuration configuration=new Configuration();
        //创建一个JOB
        Job job=Job.getInstance(configuration);
        //设置JOB对应的参数：主类
        job.setJarByClass(WorldCountLocalApp.class);
        //设置Job对应的参数：设置自定义的Mapper和Reducer处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //设置Job对应的参数：Mapper输出Key和Value的类型(Mapper的输出类型决定reducer输入类型所以reducer不用)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置Job对应的参数：reducer输出Key和Value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //如果输出目录已经存在，则先删除
       FileSystem fileSystem=FileSystem.get(configuration);
        Path outputPath=new Path("output");
       if(fileSystem.exists(outputPath)){
           fileSystem.delete(outputPath,true);
       }
        //设置Job对应的参数：Mapper输出Key和Value的类型:作业输入和输出的路径
        FileInputFormat.setInputPaths(job,new Path("input"));//读取
        FileOutputFormat.setOutputPath(job,new Path("output"));//写入

        //提交Job
        boolean result=job.waitForCompletion(true);
        System.exit(result?0:-1);
    }
}
