package com.hadoop.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/*
* MapReduce自定义分区规则
* partitioner决定maptask输出的数据交由哪个reducetask处理
* HashPartitioner默认实现：分发的key的hash值与reduce task个数取模
* */
public class AccessPartitioner  extends Partitioner<Text, AccessData> {
    /*
    * 手机号
    * */
    @Override
    public int getPartition(Text phone, AccessData access, int numReduceTasks) {
        if(phone.toString().startsWith("13")){
            return 0;
        }else if(phone.toString().startsWith("15")){
            return 1;
        }else{
            return 2;
        }
    }
}
