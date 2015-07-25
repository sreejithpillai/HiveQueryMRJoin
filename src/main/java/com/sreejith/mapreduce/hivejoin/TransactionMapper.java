package com.sreejith.mapreduce.hivejoin;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import com.sreejith.mapreduce.utils.JoinConstants;

public class TransactionMapper extends Mapper<LongWritable, Text, Text, Text>
		implements JoinConstants {
	private Text outkey = new Text();
	private Text outvalue = new Text();
	private static final Logger logger = Logger
			.getLogger(TransactionMapper.class);

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		logger.info("Initiating Mapper class for Transaction");
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] strArrText = value.toString().split(PIPE);
		outkey = new Text(strArrText[0]);
		String strValue = TRANSACTION + strArrText[0] + TILDE + strArrText[2];
		outvalue = new Text(strValue);
		context.write(outkey, outvalue);
	}

	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}
}