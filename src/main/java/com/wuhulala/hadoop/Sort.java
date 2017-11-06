package com.wuhulala.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce排序<br/>
 *
 * 1. 先创建hdfs://127.0.0.1:9000/sort_input目录 [hadoop fs -mkdir hdfs://127.0.0.1:9000/sort_input]<br/>
 * 2. 先上传resources目录下面的sort的input目录至hdfs://127.0.0.1:9000/sort_input
 * [hadoop fs -put input/* hdfs://127.0.0.1:9000/sort_input]
 <br/>
 *
 * @author wuhulala
 */
public class Sort {

    public static class Map extends
            Mapper<Object, Text, IntWritable, IntWritable> {

        private static IntWritable data = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            data.set(Integer.parseInt(line));

            context.write(data, new IntWritable(1));

        }

    }

    public static class Reduce extends
            Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private static IntWritable linenum = new IntWritable(1);

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {

            for (IntWritable val : values) {

                context.write(linenum, key);

                linenum = new IntWritable(linenum.get() + 1);
            }

        }
    }

    public static class Partition extends Partitioner<IntWritable, IntWritable> {
        /**
         * 为每一个数值进行分区
         * @param key 键
         * @param value 值
         * @param numPartitions 分区个数
         * @return 分区id
         */
        @Override
        public int getPartition(IntWritable key, IntWritable value,
                                int numPartitions) {
            int maxNumber = 65223;
            int bound = maxNumber / numPartitions + 1;
            int keyNumber = key.get();
            for (int i = 0; i < numPartitions; i++) {
                if (keyNumber < bound * i && keyNumber >= bound * (i - 1)) {
                    return i - 1;
                }
            }
            return 0;
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Sort");
        job.setJarByClass(Sort.class);
        job.setMapperClass(Map.class);
        job.setPartitionerClass(Partition.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://127.0.0.1:9000/sort_input"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://127.0.0.1:9000/sort_output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
