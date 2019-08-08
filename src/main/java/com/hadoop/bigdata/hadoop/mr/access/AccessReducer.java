package com.hadoop.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
public class AccessReducer extends Reducer<Text, AccessData, NullWritable, AccessData> {
    /*
    * Key 手机号
    * value <Access,Access>
    *     */

    @Override
    protected void reduce(Text key, Iterable<AccessData> values, Context context) throws IOException, InterruptedException {
        long ups=0;
        long downs=0;
        for(AccessData access: values){
            ups+=access.getUp();
            downs=access.getDown();

        }
        context.write(NullWritable.get(),new AccessData(key.toString(),ups,downs));
    }

}
