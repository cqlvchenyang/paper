package net.lvcy.mapreduce.ap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.lvcy.base.Edge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ResponsibilityHbase {
	public static class RHMapper extends TableMapper<Text, Edge>{
		@Override
		protected void map(ImmutableBytesWritable key,Result value,Mapper<ImmutableBytesWritable, Result, Text, Edge>.Context context)throws IOException, InterruptedException {
			Edge edge=new Edge(key,value);
			System.out.println("Availability Mapper: "+edge.toString());
			context.write(new Text(edge.getNodeOne()), edge);
		}
	}
	public static class RHReducer extends TableReducer<Text, Edge, ImmutableBytesWritable>{
		@Override
		protected void reduce(Text key,Iterable<Edge> values,Reducer<Text, Edge, ImmutableBytesWritable, Mutation>.Context context)throws IOException, InterruptedException {
			List<Edge> edges=new ArrayList<Edge>();
			Iterator<Edge> iterator=values.iterator();
			while(iterator.hasNext()){
				Edge edge=iterator.next();
				edges.add((Edge)edge.clone());
			}
			for (int i=0;i<edges.size();i++){
				
				double max=getMax(edges, i);
				double rik=edges.get(i).getSimilarity()-max;
				//¼ÆËã×èÄáÏµÊý
				edges.get(i).setResponsibility((1-0.5)*rik+0.5*edges.get(i).getResponsibility());
				System.out.println("Responsibility Reducer: "+edges.get(i).toString());
				String rowkey=edges.get(i).getNodeOne()+"-"+edges.get(i).getNodeTwo();
				Put put=new Put(rowkey.getBytes());
				put.addImmutable("similarity".getBytes(), "".getBytes(), String.valueOf(edges.get(i).getSimilarity()).getBytes());
				put.addImmutable("avaliability".getBytes(), "".getBytes(), String.valueOf(edges.get(i).getAvailability()).getBytes());
				put.addImmutable("responsibility".getBytes(), "".getBytes(), String.valueOf(edges.get(i).getResponsibility()).getBytes());
				context.write(null, put);
			}
			
			
		}
		
		private double getMax(List<Edge> edges,int index){
			double max=-Double.MAX_VALUE;
			for (int i=0;i<edges.size();i++) {
				if(i==index) continue;
				double sum=edges.get(i).getAvailability()+edges.get(i).getSimilarity();
				if(max<sum){
					max=sum;
				}
			}
			return max;
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		/*Configuration configuration=new Configuration();
		Job job=Job.getInstance(configuration,"Responsibility");
		job.setJarByClass(Responsibility.class);
		job.setMapperClass(RMapper.class);
		job.setReducerClass(RReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Edge.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));*/
		
		Configuration configuration=HBaseConfiguration.create();
		Job job=Job.getInstance(configuration, "Responsibility Hbase");
		
		Scan scan=new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		
		TableMapReduceUtil.initTableMapperJob("ap", scan, RHMapper.class, Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob("ap", RHReducer.class, job);
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
