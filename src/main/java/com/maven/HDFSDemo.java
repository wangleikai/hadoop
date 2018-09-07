package com.maven;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HDFSDemo {
	
	FileSystem fs = null;
	
	@Before
	public void init() throws Exception {
		Configuration conf = new Configuration();
		fs = FileSystem.get(new URI("hdfs://hadoop-master:9000"), conf,"root");
	}
	
	/**
	 * 往集群中上传文件
	 * @throws Exception
	 * @throws IOException
	 */
	@Test
	public void testUpload() throws Exception, IOException {
		fs.copyFromLocalFile(new Path("D:/data/rat.json"), new Path("/rat.json"));
		
	}
	/**
	 * 从集群中下载文件
	 * @throws Exception
	 * @throws Exception
	 */
	@Test
	public void testDownload() throws Exception, Exception {
		fs.copyToLocalFile(new Path("/a.txt"), new Path("E:/"));
	}
	
	/**
	 * 删除文件
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 * @throws Exception
	 */
	@Test
	public void testDelete() throws IllegalArgumentException, Exception {
		fs.delete(new Path("/app.txt"));
	}
	
	/**
	 * 创建目录
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 * @throws Exception
	 */
	@Test
	public void testMkdirs() throws IllegalArgumentException, Exception {
		fs.mkdirs(new Path("/aa/bb/cc/dd"));
	}
	
	/**
	 * 改名 移动文件
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 * @throws Exception
	 */
	@Test
	public void testRename() throws IllegalArgumentException, Exception {
		fs.rename(new Path("/a.txt"),new Path("/b.txt") );
		}
	
	/**
	 * 查看文件的状态
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 * @throws FileNotFoundException 
	 * @throws Exception
	 */
	@Test
	public void listFiles() throws FileNotFoundException, IllegalArgumentException, Exception {
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while(listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();
			System.out.println(fileStatus.getLen());//获取文件的长度
			System.out.println(fileStatus.getPath());//获取文件的路径
			System.out.println(fileStatus.getReplication());//获取副本的数量
			System.out.println(fileStatus.getBlockSize());//获取块的大小
			System.out.println(fileStatus.getAccessTime());//获取最后修改的时间
			
			System.out.println("===================================");
			//获取所有的文件块 文件块对应的偏移量信息，以及文件块的位置
			BlockLocation[] blockLocations = fileStatus.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations) {
				
				System.out.println(blockLocation);
			}
			}
	}
	
	/**
	 * 列出文件夹下的所有的文件或目录
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 * @throws FileNotFoundException 
	 */
	@Test
	public void list() throws FileNotFoundException, IllegalArgumentException, Exception {
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus) {
			System.out.println(fileStatus.getOwner());
			System.out.println(fileStatus.getAccessTime());
			System.out.println(fileStatus.getBlockSize());
			System.out.println(fileStatus.getLen());
			System.out.println(fileStatus.getPath());
			System.out.println(fileStatus.getReplication());
		}
	}
	@After
	public void close() throws Exception {
		fs.close();
	}

}
