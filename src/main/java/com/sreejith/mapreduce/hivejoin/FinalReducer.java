package com.sreejith.mapreduce.hivejoin;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import com.sreejith.mapreduce.utils.JoinConstants;

public class FinalReducer extends Reducer<Text, Text, NullWritable, Text>
		implements JoinConstants {
	private static final Logger log = Logger.getLogger(IntmdteJoinDriver.class);

	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0, count = 0;
		String tmp = "";
		Iterator<Text> val = values.iterator()

		while (val.hasNext()) {
			tmp = val.next().toString();
			sum = sum + Integer.parseInt(tmp);
			log.info("sum : " + sum);
			count++;
		}
		context.write(NullWritable.get(), new Text(key + PIPE + sum + PIPE
				+ count));
	}
}