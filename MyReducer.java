import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Arrays;
import java.util.List;
public class MyReducer
extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values,
	Context context)
	throws IOException, InterruptedException {
		int temp = Integer.MIN_VALUE;
		int maxValue = Integer.MIN_VALUE;
		int minValue = Integer.MAX_VALUE;
		int temperature=0;
		Text minLat=new Text("minLat");
		Text minLong=new Text("minLong");
		Text maxLat=new Text("maxLar");
		Text maxLong=new Text("maxLong");
		
		String[] splittedInfo;
		for (Text value : values) {
			splittedInfo = value.toString().split("_");	
			temperature = Integer.parseInt(splittedInfo[0]);
			if(temperature>maxValue){
				maxValue=temperature;
				maxLat = new Text(splittedInfo[1]);
				maxLong = new Text(splittedInfo[2]);
			}
			if(temperature<minValue){
				minValue=temperature;
				minLat = new Text(splittedInfo[1]);
				minLong = new Text(splittedInfo[2]);
			}			
		}
		context.write(key, new Text(minLat+"_"+ minLong+"_"+ minValue+"_"+maxLat+"_"+ maxLong+"_"+ maxValue)); 
	}
}
