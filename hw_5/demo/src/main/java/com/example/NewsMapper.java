package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class NewsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Set<String> stopWords = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 使用Hadoop的FileSystem来读取停词文件
        String stopWordsFile = context.getConfiguration().get("input/stop-word-list.txt");
        
        // 确保停词文件路径不为 null
        Path path = new Path(stopWordsFile);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        
        // 读取停词文件
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        while ((line = br.readLine()) != null) {
            // 只添加非空的停词
            if (line.trim().length() > 0) {
                stopWords.add(line.trim().toLowerCase()); // 确保停词为小写
            }
        }
        
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 确保输入的 value 不为 null
        if (value != null && value.toString().length() > 0) {
            String[] fields = value.toString().split(",");

            // 检查字段的长度，确保能移除第一列、最后一列和倒数第二列
            if (fields.length > 3) { // 至少需要三列
                StringBuilder titleBuilder = new StringBuilder();

                // 遍历字段，从第二列到倒数第三列
                for (int i = 1; i < fields.length - 2; i++) {
                    titleBuilder.append(fields[i].trim()).append(" "); // 拼接标题
                }

                String title = titleBuilder.toString().trim(); // 去掉多余空格
                if (!title.isEmpty()) { // 确保标题不为空
                    String[] words = title.toLowerCase().split("\\W+"); // 分词，忽略标点，统一转换为小写
                    for (String w : words) {
                        // 只处理非停词、非空词，且不包含数字或特定标点
                        if (!stopWords.contains(w) && !w.isEmpty() && isValidWord(w)) {
                            word.set(w);
                            context.write(word, one);
                        }
                    }
                } else {
                    System.err.println("Title is null or empty for value: " + value);
                }
            } else {
                System.err.println("Field length is less than expected for value: " + value);
            }
        } else {
            System.err.println("Input value is null or empty.");
        }
    }

    // 检查单词是否有效：不包含数字和特定标点
    private boolean isValidWord(String word) {
        // 仅允许字母和连字符
        return word.matches("[a-zA-Z-]+") && word.length() > 1;
    }
}
