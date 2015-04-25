package net.lvcy.mapreduce.ap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.lvcy.base.Edge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Responsibility {

	public static class RMapper extends Mapper<Object, Text, Text, Edge>{
		
		@Override
		protected void map(Object key, Text value,Mapper<Object, Text, Text, Edge>.Context context)throws IOException, InterruptedException {
			
			Edge edge=new Edge(value);
			System.out.println("Availability Mapper: "+edge.toString());
			context.write(new Text(edge.getNodeOne()), edge);
		}
		
	}
	public static class RReducer extends Reducer<Text, Edge, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Edge> values,Reducer<Text, Edge, Text, Text>.Context context)throws IOException, InterruptedException {
			
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
				context.write(new Text(edges.get(i).toString()), new Text());
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
		Configuration configuration=new Configuration();
		Job job=Job.getInstance(configuration,"Responsibility");
		job.setJarByClass(Responsibility.class);
		job.setMapperClass(RMapper.class);
		job.setReducerClass(RReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Edge.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
