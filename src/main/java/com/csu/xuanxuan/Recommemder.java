package com.csu.xuanxuan;

import java.io.IOException;

import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Recommemder {
    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] userAndFriends = line.split("\t");
            if (userAndFriends.length != 2) {
                return;
            }
            IntWritable userId = new IntWritable(Integer.parseInt(userAndFriends[0]));
            List<IntWritable> friends = new ArrayList<>();
            for (String item : userAndFriends[1].split(",")) {
                friends.add(new IntWritable(Integer.parseInt(item)));
            }
            Text friend1Value = new Text();
            Text friend2Value = new Text();
            int index = 0;
            for (IntWritable friend1 : friends) {
                friend1Value.set("1," + friend1);
                context.write(userId, friend1Value);
                ++index;
                for (IntWritable friend2 : friends.subList(index, friends.size())) {
                    friend2Value.set("2," + friend2);
                    context.write(friend1, friend2Value);
                    context.write(friend2, friend1Value);
                }
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String[] value;
            HashMap<String, Integer> commonMap = new HashMap<>();
            for (Text val : values) {
                value = val.toString().split(",");
                if (value[0].equals("1")) {
                    commonMap.put(value[1], -1);
                } else {
                    if (commonMap.containsKey(value[1])) {
                        if (commonMap.get(value[1]) != -1) {
                            commonMap.put(value[1], commonMap.get(value[1]) + 1);
                        }
                    } else {
                        commonMap.put(value[1], 1);
                    }
                }
            }
            ArrayList<Entry<String, Integer>> commonList = new ArrayList<>();
            for (Entry<String, Integer> entry : commonMap.entrySet()) {
                if (entry.getValue() != -1) {
                    commonList.add(entry);
                }
            }
            Collections.sort(commonList, new Comparator<Entry<String, Integer>>() {
                @Override
                public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
                    if (o1.getValue() == o2.getValue()) {
                        return 0;
                    } else if (o1.getValue() > o2.getValue()) {
                        return 1;
                    } else {
                        return -1;
                    }
                }
            });
            final int MAX_RECOMMENDATION_COUNT = 10;
            List<String> top = new ArrayList<>();
            for (int i = 0; i < Math.min(MAX_RECOMMENDATION_COUNT, commonList.size()); ++i) {
                top.add(commonList.get(i).getKey());
            }
            context.write(key, new Text(StringUtils.join(top, ",")));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "FriendshipRecommender");
        job.setJarByClass(Recommemder.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
