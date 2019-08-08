package com.hadoop.bigdata.hadoop.hdfs;
/*
*
* 自定义mapper
* */
public interface OperateMapper {

    /*
    *
    * line 读取到每一行数据
    * context 上下文/缓存
    * */
    public void map(String line,CacheContext context);


}
