package com.maven;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StreamDemo {

	FileSystem fs = null;
	@Before
	public void init() throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop-master:9000");
		fs = FileSystem.get(conf);
	}
	/**
	 * 将本机中的文件以流的形式上传到hdfs中
	 * @throws Exception
	 */
	@Test
	public void writeFile() throws Exception {
		FSDataOutputStream outputStream = fs.create(new Path("/write.txt"));
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("d:/data/taobao.dat"), "UTF-8")); 
		String line;
		while((line = br.readLine())!=null) {
			byte[] bs = line.getBytes();
			outputStream.write(bs);
		}
		br.close();
		outputStream.close();
	}
	
	/**
	 * 将hdfs中的文件以流的形式下载下来
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 * @throws Exception
	 */
	@SuppressWarnings("resource")
	@Test
	public void readFile() throws Exception {
		FSDataInputStream open = fs.open(new Path("/write.txt"));
		BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(new File("d:/taotao/a.txt")));
		//BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("d:/taotao/a.txt")));
		int line;
		byte[] b = new byte[1024];
		while((line = open.read(b))!= -1) {
			
			bw.write(b,0,line);
			
		}
		bw.close();
		open.close();
	}
	
	@After
	public void after() throws Exception {
		fs.close();
	}
}
