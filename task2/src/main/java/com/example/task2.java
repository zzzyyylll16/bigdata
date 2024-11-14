package com.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class task2 {

    public static class WeekdayMapper extends Mapper<Object, Text, Text, Text> {
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd", Locale.ENGLISH);
        private SimpleDateFormat sdfDay = new SimpleDateFormat("EEEE", Locale.ENGLISH);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            String dateStr = fields[0];
            String[] amounts = fields[1].split(",");
            String flowIn = amounts[0];
            String flowOut = amounts[1];

            try {
                Date date = sdf.parse(dateStr);
                String weekday = sdfDay.format(date);
                context.write(new Text(weekday), new Text(flowIn + "," + flowOut));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class WeekdayReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long totalIn = 0;
            long totalOut = 0;
            long count = 0;

            for (Text val : values) {
                String[] amounts = val.toString().split(",");
                totalIn += Long.parseLong(amounts[0]);
                totalOut += Long.parseLong(amounts[1]);
                count++;
            }

            long avgIn = totalIn / count;
            long avgOut = totalOut / count;
            context.write(key, new Text(avgIn + "," + avgOut));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weekly Flow Stats");
        job.setJarByClass(task2.class);
        job.setMapperClass(WeekdayMapper.class); 
        job.setReducerClass(WeekdayReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
