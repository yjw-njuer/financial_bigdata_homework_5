package com.example;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StockReducer extends Reducer<Text, IntWritable, Text, Text> {
    private Map<String, Integer> countMap = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable val : values) {
            count += val.get();
        }
        countMap.put(key.toString(), count);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Create a list from the map entries
        List<Map.Entry<String, Integer>> entryList = new ArrayList<>(countMap.entrySet());

        // Sort the list by count in descending order
        Collections.sort(entryList, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) {
                return e2.getValue().compareTo(e1.getValue()); // Descending order
            }
        });

        // Write output in the desired format
        int rank = 1;
        for (Map.Entry<String, Integer> entry : entryList) {
            String output = String.format("%d:%s,%d", rank++, entry.getKey(), entry.getValue());
            context.write(new Text(output), null); // Key is the formatted string, value can be null
        }
    }
}