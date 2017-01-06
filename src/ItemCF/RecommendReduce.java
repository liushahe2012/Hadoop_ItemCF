package ItemCF;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecommendReduce extends Reducer<Text, DoubleWritable, Text, Text>{

	Text k = new Text();
	Text v = new Text();
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		double totalScore = 0.0;
		for(DoubleWritable d : values)
		{
			totalScore += d.get();
		}
		String str[] = key.toString().split(":");
		k.set(str[0]);
		v.set(str[1] + ":" + totalScore);
		context.write(k, v);
				
	}
}
