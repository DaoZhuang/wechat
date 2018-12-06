package com.hadoop.test;

import java.net.URI;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsTest {

	/**
	 * 
	 * �ͻ���ȥ����hdfsʱ������һ���û���ݵ�
	 * Ĭ������£�hdfs�ͻ���api���jvm�л�ȡһ����������Ϊ�Լ����û���ݣ�-DHADOOP_USER_NAME=hadoop
	 * 
	 * Ҳ�����ڹ���ͻ���fs����ʱ��ͨ���������ݽ�ȥ
	 * @author
	 *
	 */
	    FileSystem fs = null;
	    Configuration conf = null;
	    @Before
	    public void init() throws Exception{
	        conf = new Configuration();
	        conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
	        conf.set("fs.replication", "1");
	        fs = FileSystem.get(new URI("hdfs://hadoop01:9000"),conf,"root"); //���һ������Ϊ�û���
	    }
	    @Test
	    public void testUpload() throws Exception {
	        Thread.sleep(2000);
	        fs.copyFromLocalFile(new Path("E:/authcode.jpg"), new Path("/authcode.jpg.cop"));
	        fs.close();
	    }
	     
	     	
	    @Test
	    public void testDownload() throws Exception {
	         
	        fs.copyToLocalFile(new Path("/out/part-r-00000"), new Path("E:/"));
	        fs.close();
	    }
	     
	    @Test
	    public void testConf(){
	        Iterator<Entry<String, String>> iterator = conf.iterator();
	        while (iterator.hasNext()) {
	            Entry<String, String> entry = iterator.next();
	            System.out.println(entry.getValue() + "--" + entry.getValue());//conf���ص�����
	        }
	    }
	     
	    /**
	     * ����Ŀ¼
	     */
	    @Test
	    public void makdirTest() throws Exception {
	        boolean mkdirs = fs.mkdirs(new Path("/aaa/bbb"));
	        System.out.println(mkdirs);
	    }
	     
	    /**
	     * ɾ��
	     */
	    @Test
	    public void deleteTest() throws Exception{
	        boolean delete = fs.delete(new Path("/aaa"), true);//true�� �ݹ�ɾ��
	        System.out.println(delete);
	    }
	     
	    @Test
	    public void listTest() throws Exception{
	         
	        FileStatus[] listStatus = fs.listStatus(new Path("/"));
	        for (FileStatus fileStatus : listStatus) {
	            System.err.println(fileStatus.getPath()+"================="+fileStatus.toString());
	        }
	        //��ݹ��ҵ����е��ļ�
	        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
	        while(listFiles.hasNext()){
	            LocatedFileStatus next = listFiles.next();
	            String name = next.getPath().getName();
	            Path path = next.getPath();
	            System.out.println(name + "---" + path.toString());
	        }
	    }
	     
	    public static void main(String[] args) throws Exception {
	        Configuration conf = new Configuration();
	        conf.set("fs.defaultFS", "hdfs://master:9000");
	        //�õ�һ���ļ�ϵͳ�����Ŀͻ���ʵ������
	        FileSystem fs = FileSystem.get(conf);
	         
	        fs.copyFromLocalFile(new Path("G:/access.log"), new Path("/access.log.copy"));
	        fs.close();
	    }
	
}
