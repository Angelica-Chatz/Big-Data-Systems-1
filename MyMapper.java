import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class MyMapper
extends Mapper<LongWritable, Text, Text, Text> {
	private static final int MISSING = 9999;
	@Override
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {
		String line = value.toString();
		String year = line.substring(15, 19);

		int airTemperature;
		if (line.charAt(87) == '+') {
			airTemperature = Integer.parseInt(line.substring(88, 92));
		} else {
			airTemperature = Integer.parseInt(line.substring(87, 92));
		}
		String quality = line.substring(92, 93);
		
		
		Text latitude;
        if (line.charAt(28) == '+') {
            latitude= new Text(line.substring(29, 34));
        } else {
            latitude= new Text(line.substring(28, 34));
        }
		Text longitude;
        if (line.charAt(34) == '+') {
            longitude= new Text(line.substring(35, 41));
        } else {
            longitude= new Text(line.substring(34, 41));
        }
		
		if (airTemperature != MISSING && quality.matches("[01459]")) {
			context.write(new Text(year), new Text(airTemperature+"_"+longitude+"_" + latitude  ));
		}
	}
}
