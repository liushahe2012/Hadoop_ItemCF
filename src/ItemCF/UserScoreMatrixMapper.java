package ItemCF;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * 第一步，计算用户评分矩阵
 * 输入的数据格式为:1,101,5.0
 * 将原始数据进行转换,以每行UserId为key,ItermId:Perference作为value输出
 */
public class UserScoreMatrixMapper extends Mapper<LongWritable, Text, Text, Text>{ 
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] str = line.split(",");
		if (str.length == 3) {
			Text k = new Text(str[0]);
			Text v = new Text(str[1]+":"+str[2]);
			context.write(k, v);
		}
	}

}
