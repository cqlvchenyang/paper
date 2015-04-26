package net.lvcy.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Edge implements Writable{

	
	private String nodeOne;
	private String nodeTwo;
	private double similarity;
	private double availability;
	private double responsibility;
	public Edge(){
		
	}
	public Edge(Text line){
		String[] lines=line.toString().split("\t");
		nodeOne=lines[0];
		nodeTwo=lines[1];
		similarity=Double.parseDouble(lines[2]);
		availability=lines.length>3?Double.parseDouble(lines[3]):0;
		responsibility=lines.length>4?Double.parseDouble(lines[4]):0;
	}
	public Edge(ImmutableBytesWritable key,Result value){
		String[] rowkeyArray=Bytes.toString(key.get()).split("-");
		this.nodeOne=rowkeyArray[0];
		this.nodeTwo=rowkeyArray[1];
		this.similarity=Long.parseLong(Bytes.toString(value.getValue("similarity".getBytes(), "".getBytes())));
		this.availability=Long.parseLong(Bytes.toString(value.getValue("avaliability".getBytes(), "".getBytes())));
		this.responsibility=Long.parseLong(Bytes.toString(value.getValue("responsibility".getBytes(), "".getBytes())));
	}
	public double getAvailability() {
		return availability;
	}
	public String getNodeOne() {
		return nodeOne;
	}
	public String getNodeTwo() {
		return nodeTwo;
	}
	public double getResponsibility() {
		return responsibility;
	}
	public double getSimilarity() {
		return similarity;
	}
	@Override
	public String toString() {
		return nodeOne+"\t"+nodeTwo+"\t"+similarity+"\t"+availability+"\t"+responsibility;
	}
	public void setNodeOne(String nodeOne) {
		this.nodeOne = nodeOne;
	}
	public void setNodeTwo(String nodeTwo) {
		this.nodeTwo = nodeTwo;
	}
	public void setSimilarity(double similarity) {
		this.similarity = similarity;
	}
	public void setAvailability(double availability) {
		this.availability = availability;
	}
	public void setResponsibility(double responsibility) {
		this.responsibility = responsibility;
	}
	public void readFields(DataInput in) throws IOException {
		this.nodeOne=in.readUTF();
		this.nodeTwo=in.readUTF();
		this.similarity=in.readDouble();
		this.availability=in.readDouble();
		this.responsibility=in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.nodeOne);
		out.writeUTF(this.nodeTwo);
		out.writeDouble(this.similarity);
		out.writeDouble(this.availability);
		out.writeDouble(this.responsibility);
	}
	@Override
	public Object clone() {
		Edge edge=new Edge();
		edge.nodeOne=this.nodeOne;
		edge.nodeTwo=this.nodeTwo;
		edge.similarity=this.similarity;
		edge.responsibility=this.responsibility;
		edge.availability=this.availability;
		return edge;
	}

}
