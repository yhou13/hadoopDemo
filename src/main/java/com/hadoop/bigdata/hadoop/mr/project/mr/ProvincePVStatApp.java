package com.hadoop.bigdata.hadoop.mr.project.mr;

/*
*
* 第一版本浏览量的统计
* */

import com.hadoop.bigdata.hadoop.mr.project.utils.LogParser;
import org.apache.commons.lang3.StringUtils;
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
import java.util.Map;

/*
* 省份浏览量统计
*
* */
public class ProvincePVStatApp {
    public static void main(String[] args)  {

        try {
            Configuration configuration=new Configuration();
            Job job= null;
            job = Job.getInstance(configuration);
            job.setJarByClass(ProvincePVStatApp.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);
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
    static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        private LongWritable ONE=new LongWritable(1);
        private LogParser logParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser=new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log=value.toString();
           Map<String,String> infos= logParser.parse(log);
           String ip=infos.get("ip");
           if(StringUtils.isNotBlank(ip)){
//             IPParser.RegionInfo regionInfo=  IPParser.getInstance().analyseIp(ip);
            /*   if(regionInfo!=null){
                   String province=regionInfo.getProvince();
                   context.write(new Text(province),ONE);
               }else {
                   context.write(new Text("-"),ONE);
               }*/
           }else {
               context.write(new Text("-"),ONE);
           }
        }
    }
    static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count=0;
            for(LongWritable value:values){
                count++;
            }
            context.write(key,new LongWritable(count));
        }
    }
}
