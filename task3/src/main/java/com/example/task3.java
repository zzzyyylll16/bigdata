package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class task3{

    public static class ActivityMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String userId = fields[0];
            long directPurchase = 0;
            long totalRedeem =0;
            try {
                directPurchase = Long.parseLong(fields[5]);
                totalRedeem = Long.parseLong(fields[8]);
            } catch (Exception e) {
                e.printStackTrace();
            }


            if (directPurchase > 0 || totalRedeem > 0) {
                context.write(new Text(userId), new IntWritable(1));
            }
        }
    }

    public static class ActivityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int activeDays = 0;
            for (IntWritable val : values) {
                activeDays += val.get();
            }
            context.write(key, new IntWritable(activeDays));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "User Activity Analysis");
        job.setJarByClass(task3.class);
        job.setMapperClass(ActivityMapper.class);
        job.setReducerClass(ActivityReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}