package com.sreejith.mapreduce.hivejoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import com.sreejith.mapreduce.utils.JoinConstants;

public class IntmdteJoinReducer extends
		Reducer<IntWritable, Text, NullWritable, Text> implements JoinConstants {
	private Text tmp = new Text();
	private ArrayList<Text> listC = new ArrayList<Text>();
	private ArrayList<Text> listP = new ArrayList<Text>();
	private static final Logger logger = Logger
			.getLogger(IntmdteJoinReducer.class);

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		logger.info("Inititated Reducer class for Join");
	}

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		// Clear lists
		listC.clear();
		listP.clear();

		Iterator<Text> ite = values.iterator();
		while (ite.hasNext()) {
			tmp = ite.next();
			if (tmp.charAt(0) == 'C') {
				listC.add(new Text(tmp.toString().substring(1)));
			} else if (tmp.charAt(0) == 'P') {
				listP.add(new Text(tmp.toString().substring(1)));
			}
		}

		// iterate through all our values, binning each record based on what
		// it was tagged with. Make sure to remove the tag!
		executeJoinLogic(context, key);
	}

	private void executeJoinLogic(Context context, IntWritable key)
			throws IOException, InterruptedException {
		if (!listC.isEmpty() && !listP.isEmpty()) {
			for (Text C : listC) {
				for (Text P : listP) {
					StringBuilder sb = new StringBuilder();
					String[] arrC = C.toString().split(TILDE);
					String[] arrP = P.toString().split(TILDE);
					for (int i = 0; i < arrC.length; i++) {
						sb.append(arrC[i]);
						sb.append(PIPE);
					}
					for (int j = 0; j < arrP.length; j++) {
						sb.append(arrP[j]);
						sb.append(PIPE);
					}
					sb.setLength(sb.length() - 1);
					context.write(NullWritable.get(), new Text(sb.toString()));
				}
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		logger.info("Reducer completed...");
		super.cleanup(context);
	}
}