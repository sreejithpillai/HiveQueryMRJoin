package com.sreejith.mapreduce.hivejoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import com.sreejith.mapreduce.utils.JoinConstants;

public class IntermediateJoinReducer extends
		Reducer<Text, Text, NullWritable, Text> implements JoinConstants {
	private Text tmp = new Text();
	private ArrayList<Text> listJ = new ArrayList<Text>();
	private ArrayList<Text> listT = new ArrayList<Text>();
	private static final Logger logger = Logger
			.getLogger(IntermediateJoinReducer.class);

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		logger.info("Inititated Reducer class for Join");
	}

	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// Clear lists
		listJ.clear();
		listT.clear();

		Iterator<Text> ite = values.iterator();
		while (ite.hasNext()) {
			tmp = ite.next();
			if (tmp.charAt(0) == 'J') {
				listJ.add(new Text(tmp.toString().substring(1)));
			} else if (tmp.charAt(0) == 'T') {
				listT.add(new Text(tmp.toString().substring(1)));
			}
		}

		// iterate through all our values, binning each record based on what
		// it was tagged with. Make sure to remove the tag!
		executeJoinLogic(context, key);
	}

	private void executeJoinLogic(Context context, Text key)
			throws IOException, InterruptedException {
		if (!listJ.isEmpty() && !listT.isEmpty()) {
			for (Text C : listJ) {
				for (Text P : listT) {
					StringBuilder sb = new StringBuilder();
					String[] arrC = C.toString().split(TILDE);
					String[] arrP = P.toString().split(TILDE);
					for (int i = 0; i < arrC.length; i++) {
						sb.append(arrC[i]);
						sb.append(PIPE);
					}
					for (int j = 0; j < arrP.length; j++) {
						if (j == 0)
							j = 1;
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