package com.hadoop.bigdata.hadoop.mr.access;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AccessYARNApp {
    public static void main(String[] args)  {
        try {
          Configuration configuration=new Configuration();
            Job  job = Job.getInstance(configuration);
            job.setJarByClass(AccessYARNApp.class);

            job.setMapperClass(AccessMapper.class);
            job.setReducerClass(AccessReducer.class);
//            设置自定义分区规则
            job.setPartitionerClass(AccessPartitioner.class);
//            设置reduce个数
            job.setNumReduceTasks(3);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(AccessData.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(AccessData.class);
            FileSystem fileSystem=FileSystem.get(configuration);
            Path outputPath=new Path(args[1]);
            if(fileSystem.exists(outputPath)){
                fileSystem.delete(outputPath,true);
            }
            FileInputFormat.setInputPaths(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));
            boolean result=job.waitForCompletion(true);
            System.exit(result?0:-1);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
