package com.hadoop.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;


/*
* 使用Java API操作HDFS文件系统
* 关键点：
* 1.创建configuration
* 2.获取fileSystem
* 3....HDFS API操作
* */
public class HDFSApp {
    public static final String HDFS_PATH="";
    FileSystem fileSystem=null;
    Configuration configuration=null;
    @Before
    public void setUp() throws Exception {
        System.out.println("-------------setUp----------");
        configuration=new Configuration();
        /*shell 脚本上传的 系数以你的配置为主，JAVA上传的以jar配置位置*/
        configuration.set("dfs.replication","1");
        /*
         * 构造一个访问指定HDFS系统的客户端对象
         * 第一个参数：HDFS的URI
         * 第二个参数：客户端指定配置参数
         * 第三个参数：客户端身份，就是用户名
         * */
        fileSystem=FileSystem.get(new URI(""),configuration);
    }
    /*
    *
    *  拷贝大本地文件到HDFS文件系统,(带进度条)
    * */
    @Test
    public void copyFromLocalBigFile() throws IOException {
        InputStream in =new BufferedInputStream(new FileInputStream(new File("本地路径")));
        FSDataOutputStream out=fileSystem.create(new Path("/hdfsapi/test/jdk.tgz"), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });
     IOUtils.copyBytes(in,out,4096);
    }
    /*
    * 拷贝HDFS文件到本地：下载
    * */
    @Test
    public void copyToLocalFile() throws IOException {
        Path src=new Path("/Users/rocky/data/hello.txt");
        Path dst=new Path("/hdfsapi/test/");
        fileSystem.copyToLocalFile(src,dst);
    }
    /*
     *  递归查看目标文件夹下的所有文件
     *
     * */
    public void listFilesRecursive() throws IOException {
        RemoteIterator<LocatedFileStatus> files=fileSystem.listFiles(new Path(""),true);
       while (files.hasNext()){
           LocatedFileStatus file=files.next();
           String isDir=file.isDirectory()?"文件夹":"文件";
           String permission= file.getPermission().toString();
           short replication=file.getReplication();
           Long length= file.getLen();
           String path=file.getPath().toString();
           System.out.println(isDir+"\t"+permission+"\t"+replication+"\t"+length+"\t"+path);
       }
    }
    /*
     *  查看目标文件夹下的所有文件内容
     *
     * */
    public void listFiles() throws IOException {
       FileStatus[] statusList= fileSystem.listStatus(new Path("/hdfsapi/test/"));
        for(FileStatus file :statusList){
            String isDir=file.isDirectory()?"文件夹":"文件";
           String permission= file.getPermission().toString();
           short replication=file.getReplication();
           Long length= file.getLen();
           String path=file.getPath().toString();
           System.out.println(isDir+"\t"+permission+"\t"+replication+"\t"+length+"\t"+path);
        }
    }
    /*
    * 查看文件块信息
    *
    * */
    @Test
    public void getFileBlockLocations() throws IOException {
        FileStatus fileStatus=fileSystem.getFileStatus(new Path("/hdfsapi/test/jdk.tgz"));
        BlockLocation[] blocks=fileSystem.getFileBlockLocations(fileStatus,0,fileStatus.getLen());
        for(BlockLocation block : blocks){
            for(String name:block.getNames()){
                System.out.println(name+":"+block.getOffset()+":"+block.getLength());
            }
        }
    }
    @Test
    public void delete() throws IOException {
        boolean result=fileSystem.delete(new Path("/hdfsapi/test/jdk.tgz"),true);
        System.out.println(result);
    }
    /*
     *  拷贝本地文件到HDFS文件系统
     * */
    @Test
    public void copyFromLocalFile() throws IOException {
        Path local=new Path("/User/rocky/data/hello.txt");
        Path dst=new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(local,dst);
    }
    /*
   测试文件名更改
   */
    @Test
    public void rename() throws IOException {
        Path oldPath=new Path("/hdfsapi/test/a.txt");
        Path newPath=new Path("/hdfsapi/test/c.txt");
        fileSystem.rename(oldPath,newPath);
    }
    /*
    * 创建HDFS文件夹
    * */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /*
    * 查看HDFS内容
    * */
    @Test
    public void text() throws IOException {
        FSDataInputStream in =fileSystem.open(new Path("/hdfs_version.properties"));
        IOUtils.copyBytes(in,System.out,1024);
    }
    /*
    * 创建文件
    * */
    @Test
    public void create() throws IOException {
       FSDataOutputStream out= fileSystem.create(new Path("/hdfsapi/test/a.txt"));
       out.writeUTF("hello HDFS");
       out.flush();
       out.close();
    }
    @After
    public void tearDown(){
        configuration =null;
        fileSystem =null;
        System.out.println("-------tearDown------");
    }
/*
        public static void main(String[] args) throws Exception{
        Configuration configuration=new Configuration();
        FileSystem fileSystem=FileSystem.get(new URI(""),configuration);
        Path path=new Path("/hdfsapi/test");
        boolean result= fileSystem.mkdirs(path);
        System.out.println(result);
    }*/

}
