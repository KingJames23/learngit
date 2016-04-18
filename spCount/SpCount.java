package spCount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SpCount {
	
	public static class SpMapper extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) 
				throws IOException,InterruptedException {
			
			String[] words = value.toString().split("\t");
			try {
				if(words.length != 7)
					return;
				String userID = words[1];
				String spName = words[4];
				String uploadTraffic = words[5];
				String downloadTraffic = words[6];
				context.write(new Text(userID+"\t"+spName), 
						      new Text(uploadTraffic+"\t"+downloadTraffic));
			    
			} catch (Exception e) {
			}
			
		}
	}

	public static class SpReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key,Iterable<Text> values,Context context) 
				throws IOException,InterruptedException{
			
			long upSum = 0;
			long downSum =0;
			long num = 0;
			for(Text value:values){
				String[] words = value.toString().split("\t");
				upSum += Long.parseLong(words[0]);
				downSum +=Long.parseLong(words[1]);
				num++;
				
			}
			context.write(key, new Text(num+"\t"+upSum+"\t"+downSum));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		if (args.length != 2) {
			System.err.println("USage: SpCount<input path><output path>");
			System.exit(1);
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"SpCount");
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.setJarByClass(SpCount.class);
		job.setMapperClass(SpMapper.class);
		job.setReducerClass(SpReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
