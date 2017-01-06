package ItemCF;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * 第二步，计算物品同现矩阵
 * 输入是第一步的输出
 * 在reduce阶段所做的就是根据key对value进行累加输出。
 */
public class ItemOccurenceReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Context con) throws IOException, InterruptedException {
		
		int sum = 0;
		for(IntWritable v : value)
		{
			sum += v.get();
		}
		con.write(key, new IntWritable(sum));
	}

}
