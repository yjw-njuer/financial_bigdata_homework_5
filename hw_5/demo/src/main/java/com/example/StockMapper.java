package com.example;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StockMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text stockCode = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the CSV line by commas
        String[] fields = value.toString().split(",");

        // Check if there are enough fields to avoid ArrayIndexOutOfBoundsException
        // We need at least 4 fields to access the last column (stock code)
        if (fields.length >= 4) {
            // The stock code is the last column
            String stock = fields[fields.length - 1].trim();
            stockCode.set(stock);
            context.write(stockCode, one);
        }
    }
}