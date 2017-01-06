package ItemCF;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RecommendMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	
	Text k = new Text();
	DoubleWritable v = new DoubleWritable();
	Map<String, Map<String, Double>> ItemOcurrenceMap = new HashMap<String, Map<String, Double>>();
	
	/*
	 * 读取分布式缓存中的同现矩阵进行初始化操作
	 */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
			//输入  101:101 5
			//String path = context.getCacheFiles()[0].toString();
			String path = context.getLocalCacheFiles()[0].getName();
			System.out.println("path="+path);
			File file = new File(path);
		    FileReader fileReade = new FileReader(file);	
		    BufferedReader buff = new BufferedReader(fileReade);
		    
		    String line;
		    while((line = buff.readLine()) != null)
		    {
		    	String[] str = line.split("\t");
		    	if (str.length != 2) {
					continue;
				}
		    	String[] ItemIDs = str[0].split(":");
		    	String itemId1 = ItemIDs[0];
		    	String itemId2 = ItemIDs[1];
		    	
		    	Double perference = Double.parseDouble(str[1]);
		    	Map<String, Double> ItemMap;
		    	if (ItemOcurrenceMap.containsKey(itemId1)) {
					ItemMap = ItemOcurrenceMap.get(itemId1);
				}
		    	else {
					ItemMap = new HashMap<String, Double>();
				}
		    	ItemMap.put(itemId2, perference);
		    	ItemOcurrenceMap.put(itemId1, ItemMap);
		    }
		    buff.close();
		    fileReade.close();
		
		}
	}
	
	/*
	 * 读取初始化后的map(同现矩阵),根据用户的评分记录来查找计算对应物品的喜好度
	 * 读取评分矩阵
	 */
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//输入为用户评分矩阵    1	103:2.5,101:5,102:3
		String[] str = value.toString().split("\t");
		String userId = str[0];
		
		//循环物品同现矩阵的行,计算各个物品
		for(Map.Entry<String, Map<String, Double>> rowEntry : ItemOcurrenceMap.entrySet())
		{
			//要计算用户对其喜好度的itermId
			String targetItemId = rowEntry.getKey();
			//如果该物品已经被该用户评过分,说明该用户已经看过该物品了,跳过
			if(value.toString().contains(targetItemId))
			{
				continue;
			}
			//总得分
		    double totalScore = 0.0;
		    //存储targetItemId的同现矩阵
		    Map<String, Double> curMap = rowEntry.getValue();
		    String[] strAttr = str[1].split(",");
			for(int i = 0; i < strAttr.length; i++)
			{
				String itemId = strAttr[i].split(":")[0];
				double per = Double.parseDouble(strAttr[i].split(":")[1]);
				double ocurrence = 0.0;
				if (curMap.get(itemId) != null) {
					ocurrence = curMap.get(itemId);
				}
				double score = per * ocurrence;
				totalScore += score;
			}
			k.set(userId + ":" + targetItemId);
			v.set(totalScore);
			context.write(k, v);
		}
		
	}

}
