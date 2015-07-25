package com.sreejith.mapreduce.hivejoin;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import com.sreejith.mapreduce.utils.JoinConstants;

public class PurchasesMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> implements JoinConstants {
	private IntWritable outkey = new IntWritable();
	private Text outvalue = new Text();
	private static final Logger logger = Logger.getLogger(ConsumerMapper.class);

	protected void setup(
			Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		logger.info("Initiating Mapper class for Purchases");
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String[] strArrText = value.toString().split(PIPE);
		outkey.set(Integer.parseInt(strArrText[1]));
		String strValue = PURCHASES + strArrText[0];
		outvalue = new Text(strValue);
		context.write(outkey, outvalue);
	}

	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}
}