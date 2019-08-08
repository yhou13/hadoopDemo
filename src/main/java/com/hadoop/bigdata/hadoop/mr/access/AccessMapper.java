package com.hadoop.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
* 自定义Mapper处理类
* */
public class AccessMapper extends Mapper<LongWritable, Text,Text, AccessData> {
    @Override
    protected void map(LongWritable key, Text value, Context context) {
        try {
            String[] lines= value.toString().split("    ");
            String phone=lines[1];//取出手机号
            long up=Long.parseLong(lines[lines.length-3]);//取出上行流量
            long down=Long.parseLong(lines[lines.length-2]);//取出下行流量
            AccessData access=new AccessData(phone,up,down);
            context.write(new Text(phone),access);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
