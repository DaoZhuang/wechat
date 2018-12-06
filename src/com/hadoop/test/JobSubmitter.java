package com.hadoop.test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobSubmitter {

	
	public static void main(String[] args) throws Exception {
		
		//�ڴ���������jvm�������û�hadoop �û���½
		System.setProperty("HADOOP_USER_NAME", "root");
		
		
		Configuration conf = new Configuration();
		
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
        // 2������job�ύ����ȥ����
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "hadoop01");
        // 3�����Ҫ��windowsϵͳ���������job�ύ�ͻ��˳�������Ҫ�������ƽ̨�ύ�Ĳ���
        conf.set("mapreduce.app-submission.cross-platform","true");
         
        Job job = Job.getInstance(conf);
         
        // 1����װ������jar�����ڵ�λ��
        job.setJar("d:/wc.jar");
        //job.setJarByClass(JobSubmitter.class);
         
        // 2����װ������ ����job��Ҫ���õ�Mapperʵ���ࡢReducerʵ����
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);
         
        // 3����װ����������job��Mapperʵ���ࡢReducerʵ��������Ľ�����ݵ�key��value����
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
         
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
         
         
         
        Path output = new Path("/aaa.txt");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"),conf,"root");
        if(fs.exists(output)){
            fs.delete(output, true);
        }
         
        // 4����װ����������jobҪ������������ݼ�����·�������ս�������·��
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, output);  // ע�⣺���·�����벻����
         
         
        // 5����װ��������Ҫ������reduce task������
        job.setNumReduceTasks(1);
         
        // 6���ύjob��yarn
        boolean res = job.waitForCompletion(true);
         
        System.exit(res?0:-1);
		
	}
}
