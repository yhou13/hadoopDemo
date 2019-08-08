# hadoopDemo
新手入门大数据电商统计demo

运行DEMO请安装hadoop本地环境



统计页面的浏览量
    count
    一行记录做成一个固定的key,value赋值为1

统计各个省份的浏览量
        select  province count(1) from XXX  group by province;
        地市信息我们是可以通过IP解析得到的 <==ip如何转换成城市信息
        ip解析：收费

统计页面的访问量:把符合规则的pageId获取到，然后进行统计即可

===》存在的问题：每个MR作业都去全量读取待处理的原始日志，如果数据量很大，是不是会疯了？
==》ETL：全量数据不方便直接进行计算的，最好是进行一步处理后再进行相应的维度统计分析
    解析出你需要的字段：ip==>城市信息
    去除一些你不需要的字段：不需要的字段救太多了。。
    ip/time/url/page_id/contry/province/city

大数据处理完以后的数据我们现在是存放在HDFS之上
sqoop:把hdfs上的统计结果导出到mysql上

hive
HDFS上的文件并没有schema的概念
 HIVE底层执行引擎支持：MR/Tez/spark
 统一元数据管理：
    hive数据是存放在HDFS
    元数据信息（记录数据的数据）是存放在MYsql中
    sql on hadoop: hive\spark sql\impala

 Hive体系架构
    client :shell,thrift/jdbc(server/jdbc),webUi(HUE/zeppelin)

 MANAGED_TABLE:内部表
    删除表：HDFS上的数据被删除&Meta（存在MYsql）也被删除

 EXTERNAL_TABLE:外部表
    HDFS上的数据不删除 & Meta(存在mysql)被删除
