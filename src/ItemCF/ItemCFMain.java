package ItemCF;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class ItemCFMain {

	public static void main(String[] args) throws Exception {
		
		//1.计算评分矩阵
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
	    if (otherArgs.length != 4) {
			System.err.println("Usage: ItemCFMain <in> <temp1> <temp2> <out>");
			System.exit(2);
		}
	    Job job1 = new Job(conf, "UserScoreMatrix");
	    job1.setJarByClass(ItemCFMain.class);
	    job1.setMapperClass(UserScoreMatrixMapper.class);
	    job1.setReducerClass(UserSocreMatrixReduce.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
	    
	    if (job1.waitForCompletion(true)) {
	    	//2.计算同现矩阵
			Job job2 = new Job(conf, "ItemOccurence");
			job2.setJarByClass(ItemCFMain.class);
			job2.setMapperClass(ItemOccurenceMapper.class);
			job2.setReducerClass(ItemOccurenceReduce.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
			
			
			//3.计算推荐结果=评分矩阵*同现矩阵
			if (job2.waitForCompletion(true)) {
				Job job3 = new Job(conf, "ItemCF");
				job3.setJarByClass(ItemCFMain.class);
				job3.setMapperClass(RecommendMapper.class);
				job3.setReducerClass(RecommendReduce.class);
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(Text.class);
				job3.setMapOutputValueClass(DoubleWritable.class);
				
				job3.addCacheFile(new URI(otherArgs[2]+"/part-r-00000"));
				FileInputFormat.addInputPath(job3, new Path(otherArgs[1]));
				FileOutputFormat.setOutputPath(job3, new Path(otherArgs[3]));
				
				System.exit(job3.waitForCompletion(true)? 0 : 1);
			}
			
			System.exit(job2.waitForCompletion(true)? 0 : 1);
			
		}
	    System.exit(job1.waitForCompletion(true)? 0 : 1);
	
	}
}
