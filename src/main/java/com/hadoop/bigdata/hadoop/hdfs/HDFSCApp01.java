package com.hadoop.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/*
* 使用HDFS API完成wordcount统计
*
* 需求：统计HDFS上的文件
*
* 功能拆解：
* 1.读取HDFS上的文件==》hdfs api
* 2.业务处理（词频统计）：对文件中的每一行数据都要进行业务处理（按照分隔符分割）==>Mapper
* 3.将处理结果缓存起来==>context
* 4.将结果输出到HDFS ==>hdfs API
*
* */
public class HDFSCApp01 {

    public static void main(String[] arg) throws Exception {
        //1 读取HDFS上的文件==》HDFS api
        Path input=new Path(ParamsUtils.getProperties().getProperty(Contant.INPUT_PATH));
        //获取要操作的HDFS文件系统
        FileSystem fs=FileSystem.get(new URI(ParamsUtils.getProperties().getProperty(Contant.HDFS_URI)),new Configuration(),"hadoop");
        RemoteIterator<LocatedFileStatus> iterator=fs.listFiles(input,false);
        //用反射获取类
        Class<?> clazz=Class.forName(ParamsUtils.getProperties().getProperty(Contant.MAPPER_CLASS));
        OperateMapper mapper=(OperateMapper)clazz.newInstance();
        //WordCountMapper mapper=new WordCountMapper();
        CacheContext context=new CacheContext();
        while (iterator.hasNext()){
            LocatedFileStatus file=iterator.next();
            FSDataInputStream in =fs.open(file.getPath());
            BufferedReader reader=new BufferedReader(new InputStreamReader(in));
            String line="";
            while ((line=reader.readLine())!=null){
                //2 业务处理（词频统计）（hello,3）
                mapper.map(line,context);
            }
            reader.close();
            in.close();
        }
        //3 将处理结果缓存起来 Map
        Map<Object,Object> contextMap=context.getCacheMap();

        //4将结果输出到HDFS==》HDFS API
        Path output=new Path(ParamsUtils.getProperties().getProperty(Contant.OUTPUT_PATH));
        FSDataOutputStream out=fs.create(new Path(output,new Path(ParamsUtils.getProperties().getProperty(Contant.OUTPUT_FILE))));
        //将第三步缓存中的内容输出到out中去
        Set<Map.Entry<Object,Object>> entries=contextMap.entrySet();
        for(Map.Entry<Object,Object> entry:entries){
            out.write((entry.getKey().toString()+"\t"+entry.getValue()+"\n").getBytes());
        }
        out.flush();
        out.close();

    }
}
