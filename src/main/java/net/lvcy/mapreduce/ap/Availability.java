package net.lvcy.mapreduce.ap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

public class Availability {

	public static class AMapper extends Mapper<Object, Text, Text, Edge>{
		@Override
		protected void map(Object key, Text value,Mapper<Object, Text, Text, Edge>.Context context)throws IOException, InterruptedException {
			
			Edge edge=new Edge(value);
			System.out.println("Availability Mapper: "+edge.toString());
			context.write(new Text(edge.getNodeTwo()), (Edge)edge.clone());
		}
	}
	public static class AReducer extends Reducer<Text, Edge, Text, Object>{
		@Override
		protected void reduce(Text key, Iterable<Edge> values,Reducer<Text, Edge, Text, Object>.Context context)throws IOException, InterruptedException {

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
				context.write(new Text(edges.get(i).toString()), "");
			}
			edges=null;
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration=new Configuration();
		Job job=Job.getInstance(configuration,"Availability");
		job.setJarByClass(Availability.class);
		job.setMapperClass(AMapper.class);
		job.setReducerClass(AReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Edge.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
