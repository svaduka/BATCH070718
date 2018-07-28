package com.laboros;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//java com.laboros.HDFSServicePut WordCount.txt /user/trainings
public class HDFSServicePut extends Configured implements Tool {

	public static void main(String[] args) {
		System.out.println("IN MAIN");
		// Step-1 : Validate
		if (args.length < 2) {
			System.out.println(
					"java Usage: HDFSServicePut [generic options] /path/to/local/file /hdfs/destination/directory");
			return;
		}
		// Step-2 : Load the configuration
		Configuration conf = new Configuration(Boolean.TRUE);
		try {
			int i= ToolRunner.run(conf, new HDFSServicePut(), args);
			if(i==0) {
				System.out.println("SUCCESS");
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("FAILURE");
		}
		

	}

	public int run(String[] args) throws Exception {
		System.out.println("IN RUN");
		
		//Dataset Write = Metadata Write + DataWrite
		
		//Step-1 : Metadata Write = Create Empty File + Add to namenode
		
		//Step-1 a : Create Empty File
		
		String edgeNodeFile = args[0]; //Wordcount.txt
		String hdfsDestDir = args[1]; // /user/trainings
		
		//Step-1 b : convert into path : HDFS every file is a URI
		Path emptyFileNameWithHdfsDestDir = new Path(hdfsDestDir, edgeNodeFile);// /user/trainings/Wordcount.txt
		//Step-1 c : Connect to Namenode, we need filesystem
		FileSystem hdfs = FileSystem.get(getConf());
		
		//Step-1 D : Connect to namenode
		FSDataOutputStream fsdos=hdfs.create(emptyFileNameWithHdfsDestDir);
		
		//Step-2 : Add data
		/**
		 * 1) Split Data into blocks
		 * 2) Identify the datanode for each datablock
		 * 3) Write the original data block to datanode
		 * 4) Meet replication
		 * 5) Sync with Namenode metadata
		 * 6) Handle Failures
		 */
		
		//Step-2-1: Split Data into blocks
			//a: Open input stream on local file
		InputStream in = new FileInputStream(edgeNodeFile);
		/**
		 * 2) Identify the datanode for each datablock
		 * 3) Write the original data block to datanode
		 * 4) Meet replication
		 * 5) Sync with Namenode metadata
		 * 6) Handle Failures
		 */
		IOUtils.copyBytes(in, fsdos, getConf(), true);
		return 0;
	}

}
