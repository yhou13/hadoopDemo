package com.hadoop.bigdata.hadoop.hdfs;


/*
*
* 自定义wc实现类（业务处理类）
* */
public class WordCountMapper implements OperateMapper{
    @Override
    public void map(String line, CacheContext context) {
        String[] words=line.toLowerCase().split("\t");
        for (String word:words){
            Object value=context.get(word);
            if(value==null){//表示没有出现过该单词
                context.write(word,1);
            }else {
                int v=Integer.parseInt(value.toString());
                context.write(word,v+1);//取出单词对应的次数+1
            }
        }
    }
}
