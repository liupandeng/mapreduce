package cn.edu360.mr.friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class CommonFriendsAll {

	public static class CommonFriendsAllMapper extends Mapper<LongWritable, Text, Text, Text>{
		Text k = new Text();
		Text v = new Text();

		// 输出：　B-C	A
		//B-D	A
		//B-F	A
		//B-G	A


		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] userPartAndFriend = value.toString().split("\t");
			String userPart = userPartAndFriend[0];
			String friend = userPartAndFriend[1];
			k.set(userPart);
			v.set(friend);
			context.write(k, v);
		}
	}



	public static class CommonFriendsAllReducer extends Reducer<Text, Text, Text, Text>{

		// 一组数据：  B -->  A  E  F  J .....
		// 一组数据：  C -->  B  F  E  J .....
		@Override
		protected void reduce(Text userPart, Iterable<Text> friends, Context context)
				throws IOException, InterruptedException {
			
			StringBuilder sb = new StringBuilder();
			for (Text friend :friends) {
				sb.append(friend).append(",");
			}

			context.write(userPart,new Text(sb.toString()));
		}
		
	}
	
	
public static void main(String[] args) throws Exception {

		
		Configuration conf = new Configuration();  
		
		Job job = Job.getInstance(conf);

		job.setJarByClass(CommonFriendsAll.class);

		job.setMapperClass(CommonFriendsAllMapper.class);
		job.setReducerClass(CommonFriendsAllReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path("F:\\mrdata\\friends\\out1"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\mrdata\\friends\\out2"));

		job.waitForCompletion(true);
	}
}
