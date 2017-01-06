package ItemCF;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * 第二步，计算物品同现矩阵
 * 输入是第一步的输出
 * 输入的key为偏移量，输入的value为UserId+制表符+ItermId1:Perference1,ItermId2:Perference2… 
 * 输入的value中，UserId和Perference是不需要关心的，观察物品的同现矩阵
 * map阶段的工作就是将每行包含的ItermId都解析出来，全排列组合作为key输出，每个key的value记为1。 
 */
public class ItemOccurenceMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//输入的数据格式为:1	103:2.5,101:5.0,102:3.0
		String line = value.toString();
		String[] str = line.split("\t");
		if(str.length != 2)
		{
			return;
		}
		
		String[] strTemp = str[1].split(",");
		for(int i = 0; i < strTemp.length; i++)
		{
			String item1 = strTemp[i].split(":")[0];
			for(int j = 0; j < strTemp.length; j++)
			{
				String item2 = strTemp[j].split(":")[0];
				Text k = new Text(item1+":"+item2);
				context.write(k, new IntWritable(1));
			}
		}

	}
}
