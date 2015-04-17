package net.lvcy.mapreduce.preprocess;

import java.io.IOException;

import net.lvcy.common.JedisUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import redis.clients.jedis.Jedis;

public class EdgesToAdjacents {

	public static class ETAMapper extends Mapper<Object, Text, Text, Text>{
		@Override
		protected void map(Object key, Text value,Mapper<Object, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			//解析输入的行
			String[] line=value.toString().split("\t");
			System.out.println(line[0]+"\t"+line[1]);
			context.write(new Text(line[0]), new Text(line[1]));
			context.write(new Text(line[1]), new Text(line[0]));
		}
	}
	public static class ETAReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Reducer<Text, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			Jedis jedis=JedisUtils.getJedis();
			//将key写入文件，邻接表写入redis
			String result="";
			for (Text value : values) {
				System.out.println("value:"+value.toString());
				result+=value.toString()+"\t";
			}
			System.out.println("result:"+result);
			jedis.set("node:"+key, result);
			context.write(key, new Text());
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration=new Configuration();
		Job job=Job.getInstance(configuration,"Edges to adjacents");
		job.setJarByClass(EdgesToAdjacents.class);
		job.setMapperClass(ETAMapper.class);
		//job.setCombinerClass(ETAReducer.class);
		job.setReducerClass(ETAReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
