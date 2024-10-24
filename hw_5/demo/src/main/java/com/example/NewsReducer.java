package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class NewsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private TreeMap<Integer, Text> countMap = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        countMap.put(sum, new Text(key)); // 将计数和单词存储在TreeMap中，按计数排序
        if (countMap.size() > 100) {
            countMap.remove(countMap.firstKey()); // 保持前100个
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int rank = 1;
        for (Map.Entry<Integer, Text> entry : countMap.descendingMap().entrySet()) {
            // 输出格式为 "<排名>：<单词>，<次数>"
            String output = rank + ":" + entry.getValue().toString() + "," + entry.getKey();
            context.write(new Text(output), null);
            rank++;
        }
    }
}
