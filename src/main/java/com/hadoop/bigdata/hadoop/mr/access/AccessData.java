package com.hadoop.bigdata.hadoop.mr.access;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
* 自定义复杂数据类型
* 1.按照hadoop的规范，需要实现Writable接口
* 2.按照hadoop的规范，需要实现Write和readFields这两个方法
* 3.定义一个默认的构造方法
* */
public class AccessData implements Writable {
    private String phone;
    private long up;
    private long down;
    private long sum;

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getUp() {
        return up;
    }

    public void setUp(long up) {
        this.up = up;
    }

    public long getDown() {
        return down;
    }

    public void setDown(long down) {
        this.down = down;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    @Override
    public String toString() {
        return "AccessData{" +
                "phone='" + phone + '\'' +
                ", up=" + up +
                ", down=" + down +
                ", sum=" + sum +
                '}';
    }

    public AccessData() {
    }
    public AccessData(String phone, long up, long down) {
        this.phone=phone;
        this.up=up;
        this.down=down;
        this.sum=up+down;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone=in.readUTF();
        this.up=in.readLong();
        this.down=in.readLong();
        this.sum=in.readLong();
    }
}
