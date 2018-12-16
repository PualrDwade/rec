package com.csu.xuanxuan;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;


/**
 * Hadoop单元测试
 */
public class RecommemderTest {

    MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;
    ReduceDriver<IntWritable, Text, IntWritable, Text> reduceDriver;
    MapReduceDriver<LongWritable, Text, IntWritable, Text, IntWritable, Text> mapReduceDriver;


    @Before
    public void setUp() {
        Recommemder.Map mapper = new Recommemder.Map();
        Recommemder.Reduce reducer = new Recommemder.Reduce();
        mapDriver = new MapDriver(mapper);
        reduceDriver = new ReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }


    /**
     * 对map函数进行单元测试
     *
     * @throws IOException
     */
    @Test
    public void testMap() throws IOException {
        //创建测试数据
        Text text = new Text("0\t1,3,5");
        mapDriver.withInput(new LongWritable(), text)
                .withOutput(new IntWritable(0), new Text("1,1"))
                .withOutput(new IntWritable(1), new Text("2,3"))
                .withOutput(new IntWritable(3), new Text("2,1"))
                .withOutput(new IntWritable(1), new Text("2,5"))
                .withOutput(new IntWritable(5), new Text("2,1"))
                .withOutput(new IntWritable(0), new Text("1,3"))
                .withOutput(new IntWritable(3), new Text("2,5"))
                .withOutput(new IntWritable(5), new Text("2,3"))
                .withOutput(new IntWritable(0), new Text("1,5"))
                .runTest();
    }


    /**
     * 对reduce函数进行单元测试
     *
     * @throws IOException
     */
    @Test
    public void testReducer() throws IOException {
        //创建测试数据
        List<Text> list = new ArrayList<>();
        list.add(new Text("1,3"));
        list.add(new Text("2,4"));
        list.add(new Text("1,5"));
        list.add(new Text("2,8"));
        list.add(new Text("2,10"));
        reduceDriver.withInput(new IntWritable(0), list)
                .withOutput(new IntWritable(0), new Text("4,8,10"))
                .runTest();
    }


    /**
     * 对整个mapreduce进行单元测试
     *
     * @throws IOException
     */
    @Test
    public void testMapReduce() throws IOException {
        //创建测试数据
        List<Pair<LongWritable, Text>> list = new ArrayList<>();
        Text text1 = new Text("0\t1");
        Text text2 = new Text("1\t0,2,3");
        Text text3 = new Text("2\t1,3");
        Text text4 = new Text("3\t1,2");
        list.add(new Pair(new LongWritable(0), text1));
        list.add(new Pair(new LongWritable(1), text2));
        list.add(new Pair(new LongWritable(2), text3));
        list.add(new Pair(new LongWritable(3), text4));
        mapReduceDriver.withAll(list)
                .withOutput(new IntWritable(0), new Text("2,3"))
                .withOutput(new IntWritable(1), new Text(""))
                .withOutput(new IntWritable(2), new Text("0"))
                .withOutput(new IntWritable(3), new Text("0"))
                .runTest();
    }
}