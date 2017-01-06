package ItemCF;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * 第一步，计算用户评分矩阵
 * 将map的输出聚合为用户评分矩阵输出
 * 将UserId相同的所有评分记录进行汇总拼接，输出的key仍然为1，value形如：101:5,102:3,103:2.5
 */
public class UserSocreMatrixReduce extends Reducer<Text, Text, Text, Text>{

	Text v = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		
		String str = new String();
		for(Text v : value)
		{
			str += ",";
			str += v.toString();
		}
		v.set(str.replaceFirst(",", ""));
		context.write(key, v);
	}
}
