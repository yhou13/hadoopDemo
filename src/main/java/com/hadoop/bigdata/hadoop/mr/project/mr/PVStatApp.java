package com.hadoop.bigdata.hadoop.mr.project.mr;

/*
*
* 第一版本浏览量的统计
* */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PVStatApp {
    public static void main(String[] args)  {

        try {
            Configuration configuration=new Configuration();
            Job job= null;
            job = Job.getInstance(configuration);
            job.setJarByClass(PVStatApp.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(LongWritable.class);
            FileSystem fileSystem=FileSystem.get(configuration);
            Path outputPath=new Path("raw/output");
            if(fileSystem.exists(outputPath)){
                fileSystem.delete(outputPath,true);
            }
            FileInputFormat.setInputPaths(job,new Path("raw/input"));
            FileOutputFormat.setOutputPath(job,new Path("raw/output"));
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
    static class MyMapper extends Mapper<LongWritable, Text,Text,LongWritable>{
        private Text KEY= new Text("key");
        private LongWritable ONE=new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) {
            try {
                context.write(KEY,ONE);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    static class MyReduce extends Reducer<Text,LongWritable, NullWritable,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) {
            long count=0;
            for(LongWritable value:values){
                count++;
            }
            try {
                context.write(NullWritable.get(),new LongWritable(count));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
