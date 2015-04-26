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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
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
				context.write(new Text(curNode), new Text(entry.getKey()+"\t"+String.format("%.4f", entry.getValue().p)));
			}
			
		}
	}
	/**
	 * 
	 * 需要证明 具有相似度的边与图的总边数比例小于1
	 *
	 */
	
	public static class LPRHReducer extends TableReducer<Text, Text, ImmutableBytesWritable>{
		@Override
		protected void reduce(Text key,Iterable<Text> values,Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)throws IOException, InterruptedException {
			for (Text value : values) {
				String[] ns=value.toString().split("\t");
				String rowkey=key.toString()+"-"+ns[0];
				double sim=-1/Double.parseDouble(ns[1]);
				if(key.toString().equals(ns[0])){
					sim=0.0000;
				}
				Put put=new Put(rowkey.getBytes());
				long ts=System.currentTimeMillis();
				put.addImmutable("similarity".getBytes(), "".getBytes(), ts, String.valueOf(sim).getBytes());
				put.addImmutable("availability".getBytes(), "".getBytes(), ts, String.valueOf(0.0).getBytes());
				put.addImmutable("responsibility".getBytes(), "".getBytes(), ts, String.valueOf(0.0).getBytes());
				context.write(null,put);
			}
			
		}
	}
	
	/*public static class LPRReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Reducer<Text, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			for (Text value : values) {
				String[] ns=value.toString().split("\t");
				if(key.toString().equals(ns[0])){
					double sim=0.000;
					Text v=new Text(ns[0]+"\t"+String.valueOf(sim));
					System.out.println("Similarity: "+key.toString()+"\t"+v.toString());
					context.write(key, v);
				}
				else if(Double.parseDouble(ns[1])>0.000){
					double sim=-1/Double.parseDouble(ns[1]);
					//将相似性存入Redis，以便后面取中值
					
					Text v=new Text(ns[0]+"\t"+String.valueOf(sim));
					
					System.out.println("Similarity: "+key.toString()+"\t"+v.toString());
					
					//将相似性存入Hbase中
					context.write(key, v);
				}
			}
		}
	}*/
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration=HBaseConfiguration.create();
		Job job=Job.getInstance(configuration, "Local PageRank");
		job.setJarByClass(LocalPageRank.class);
		job.setMapperClass(LPRMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		TableMapReduceUtil.initTableReducerJob("ap", LPRHReducer.class, job);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
