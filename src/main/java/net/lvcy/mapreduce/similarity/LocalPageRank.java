package net.lvcy.mapreduce.similarity;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.lvcy.base.PageRank;
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

public class LocalPageRank {

	public static class LPRMapper extends Mapper<Object, Text, Text, Text>{
		@Override
		protected void map(Object key, Text value,Mapper<Object, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			Jedis jedis=JedisUtils.getJedis();
			/*定义变量*/
			final double threshold=10e-4;
			final double factor=0.20;
			Map<String, PageRank> pageRankMap=new HashMap<String, PageRank>();
			Set<String> buffer=new HashSet<String>();
			
			String curNode=value.toString().trim();
			buffer.add(curNode);
			pageRankMap.put(curNode, new PageRank(0.0, 1.0));
			
			while(buffer.size()>0){
				String node=buffer.iterator().next();
				List<String> adjacents=Arrays.asList(jedis.get("node:"+node).split("\t"));
				long count=adjacents.size();
				double p=pageRankMap.get(node).p;
				double r=pageRankMap.get(node).r;
				
				if(r/count<=threshold){
					buffer.remove(node);
					continue;
				}
				
				p=p+factor*r;
				r=(1-factor)*r/2;
				pageRankMap.get(node).p=p;
				pageRankMap.get(node).r=r;
				double inr=(1-factor)*r/(2*count);
				
				for (String adjacent : adjacents) {
					if(pageRankMap.containsKey(adjacent)){
						pageRankMap.get(adjacent).r=pageRankMap.get(adjacent).r+inr;
					}else{
						PageRank pr=new PageRank(0.0, inr);
						pageRankMap.put(adjacent, pr);
					}
					buffer.add(adjacent);
				}
				
			}
			
			Iterator<Entry<String, PageRank>> iterator=pageRankMap.entrySet().iterator();
			while(iterator.hasNext()){
				Entry<String, PageRank> entry=iterator.next();
				context.write(new Text(curNode), new Text(entry.getKey()+"\t"+String.format("%.3f", entry.getValue().p)));
			}
			
		}
	}
	public static class LPRReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Reducer<Text, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration=new Configuration();
		Job job=Job.getInstance(configuration, "Local PageRank");
		job.setJarByClass(LocalPageRank.class);
		job.setMapperClass(LPRMapper.class);
		job.setReducerClass(LPRReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
