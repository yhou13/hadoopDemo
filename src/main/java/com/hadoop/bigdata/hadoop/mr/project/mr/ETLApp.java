package com.hadoop.bigdata.hadoop.mr.project.mr;

import com.hadoop.bigdata.hadoop.mr.project.utils.ContentUtils;
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

public class ETLApp {
    public static void main(String[] args)  {

        try {
            Configuration configuration=new Configuration();
            Job job= null;
            job = Job.getInstance(configuration);
            job.setJarByClass(ETLApp.class);
            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);

            FileSystem fileSystem=FileSystem.get(configuration);
            Path outputPath=new Path("raw/output");
            if(fileSystem.exists(outputPath)){
                fileSystem.delete(outputPath,true);
            }
            FileInputFormat.setInputPaths(job,new Path("raw/input"));
            FileOutputFormat.setOutputPath(job,new Path("raw/input/etl"));
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
    static class MyMapper extends Mapper<LongWritable,Text,NullWritable,Text> {
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
            String country=infos.get("country");
            String province=infos.get("province");
            String city=infos.get("city");
            String url=infos.get("url");
            String time=infos.get("time");
            String pageId=ContentUtils.getPageId(url);
            StringBuilder builder=new StringBuilder();
            builder.append(ip).append("\t");
            builder.append(country).append("\t");
            builder.append(province).append("\t");
            builder.append(city).append("\t");
            builder.append(url).append("\t");
            builder.append(time).append("\t");
            builder.append(pageId).append("\t");
            context.write(NullWritable.get(),new Text(builder.toString()));


        }
    }
}
