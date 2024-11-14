package com.shiyan2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class task1 {

    public static class FundFlowMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String date = fields[1]; // 假设交易日期是第二个字段
            
            // 注意字段索引和空值处理
            long purchaseAmt = 0;
            long redeemAmt = 0;
            try {
                purchaseAmt = fields[4].isEmpty() ? 0 : Long.parseLong(fields[4].trim()); // 资金流入
                redeemAmt = fields[8].isEmpty() ? 0 : Long.parseLong(fields[8].trim()); // 资金流出
            } catch (NumberFormatException e) {
                // 如果解析失败，则忽略该记录，记录为0
                System.err.println("Error parsing amount for record: " + value.toString());
            }
            
            context.write(new Text(date), new Text(String.valueOf(purchaseAmt) + "," + String.valueOf(redeemAmt)));
        }
    }

    public static class FundFlowReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long totalPurchase = 0;
            long totalRedeem = 0;
            
            for (Text value : values) {
                String[] amounts = value.toString().split(",");
                if (amounts.length == 2) {
                    totalPurchase += Long.parseLong(amounts[0]);
                    totalRedeem += Long.parseLong(amounts[1]);
                } else {
                    System.err.println("Invalid value format: " + value.toString());
                }
            }
            
            context.write(key, new Text(String.valueOf(totalPurchase) + "," + String.valueOf(totalRedeem)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Daily Fund Flow Statistics");
        job.setJarByClass(task1.class);
        job.setMapperClass(FundFlowMapper.class);
        job.setReducerClass(FundFlowReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}