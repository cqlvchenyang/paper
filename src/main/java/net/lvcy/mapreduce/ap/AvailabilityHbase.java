package net.lvcy.mapreduce.ap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.lvcy.base.Edge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AvailabilityHbase {

	public static class AHMapper extends TableMapper<Text, Edge>{
		@Override
		protected void map(ImmutableBytesWritable key,Result value,Mapper<ImmutableBytesWritable, Result, Text, Edge>.Context context)throws IOException, InterruptedException {
			Edge edge=new Edge(key, value);
			context.write(new Text(edge.getNodeTwo()), edge);
		}
	}
	public static class AHReducer extends TableReducer<Text, Edge, ImmutableBytesWritable>{
		@Override
		protected void reduce(Text key,Iterable<Edge> values,Reducer<Text, Edge, ImmutableBytesWritable, Mutation>.Context context)throws IOException, InterruptedException {
			List<Edge> edges=new ArrayList<Edge>();
			Iterator<Edge> iterator=values.iterator();
			while(iterator.hasNext()){
				Edge edge=iterator.next();
				edges.add((Edge)edge.clone());
				
			}
			double rkk=0;
			double sum_rjk=0;
			for(int i=0;i<edges.size();i++){
				if(edges.get(i).getNodeOne().equals(edges.get(i).getNodeTwo())){
					rkk=edges.get(i).getResponsibility();
				}else if (edges.get(i).getResponsibility()>0) {
					sum_rjk+=edges.get(i).getResponsibility();
				}
			}
			double aik=0;
			for(int i=0;i<edges.size();i++){
				
				if(edges.get(i).getNodeOne().equals(edges.get(i).getNodeTwo())){
					aik=sum_rjk;
				}else {
					if(edges.get(i).getResponsibility()>0){
						aik=sum_rjk-edges.get(i).getResponsibility();
					}
					aik+=rkk;
					if(aik<0){
						edges.get(i).setAvailability(aik);
					}else {
						edges.get(i).setAvailability(0);
					}
				}
				edges.get(i).setAvailability(aik);
				//¼ÆËã×èÄáÏµÊý
				//edges.get(i).setResponsibility((1-0.5)*edges.get(i).getResponsibility()+0.5*edges.get(i).getResponsibility());
				edges.get(i).setAvailability((1-0.5)*aik+0.5*edges.get(i).getAvailability());
				
				System.out.println("Availability Reducer: "+edges.get(i));
				
				Edge edge=edges.get(i);
				String rowkey=edge.getNodeOne()+"-"+edge.getNodeTwo();
				double similarity=edge.getSimilarity();
				double availability=edge.getAvailability();
				double responsibility=edge.getResponsibility();
				Put put = new Put(rowkey.getBytes());
				put.addColumn(Bytes.toBytes("similarity"), Bytes.toBytes(""), Bytes.toBytes(similarity));
				put.addColumn(Bytes.toBytes("availability"), Bytes.toBytes(""), Bytes.toBytes(availability));
				put.addColumn(Bytes.toBytes("responsibility"), Bytes.toBytes(""), Bytes.toBytes(responsibility));
				context.write(null,put);
			}
			edges=null;
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration=HBaseConfiguration.create();
		Job job=Job.getInstance(configuration,"Availability Hbase");
		job.setJarByClass(AvailabilityHbase.class);
		
		Scan scan=new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		
		TableMapReduceUtil.initTableMapperJob("ap", scan, AHMapper.class, Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob("ap", AHReducer.class, job);
		job.setNumReduceTasks(1);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
