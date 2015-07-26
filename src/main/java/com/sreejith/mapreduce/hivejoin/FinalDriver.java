package com.sreejith.mapreduce.hivejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class FinalDriver extends Configured implements Tool {
	private Configuration conf1;
	private Configuration conf2;
	private Configuration conf3;
	private static final Logger log = Logger.getLogger(IntmdteJoinDriver.class);

	public int run(String[] args) throws Exception {
		String inputLoc1 = args[0];
		String inputLoc2 = args[1];
		String inputLoc3 = args[2];
		String outputLoc = args[3];
		String outputLoc2 = args[4];
		String finalOutput = args[5];

		// Job Chaning of multiple jobs.
		// Executing Job 1.
		conf1 = getConf();
		conf1.set("mapred.job.queue.name", "sree");

		Job job1 = new Job(conf1,
				"MapReduce: Performing Cross Join for 2 tables ");
		job1.setJarByClass(FinalDriver.class);
		MultipleInputs.addInputPath(job1, new Path(inputLoc1),
				TextInputFormat.class, ConsumerMapper.class);
		MultipleInputs.addInputPath(job1, new Path(inputLoc2),
				TextInputFormat.class, PurchasesMapper.class);
		job1.setReducerClass(IntmdteJoinReducer.class);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(Text.class);
		log.info("Input Path to the map-reduce job " + inputLoc1 + " and  "
				+ inputLoc2);
		log.info("Ouput Path to the map-reduce job " + outputLoc);
		Path output = new Path(outputLoc);
		FileOutputFormat.setOutputPath(job1, output);
		job1.waitForCompletion(true);

		// Job 2
		conf2 = getConf();
		conf2.set("mapred.job.queue.name", "sree");
		Job job2 = new Job(conf2,
				"MapReduce: Performing Cross Join for Final table");
		job2.setJarByClass(FinalDriver.class);
		MultipleInputs.addInputPath(job2, new Path(outputLoc),
				TextInputFormat.class, JoinResultMapper.class);
		MultipleInputs.addInputPath(job2, new Path(inputLoc3),
				TextInputFormat.class, TransactionMapper.class);
		job2.setReducerClass(IntermediateJoinReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(Text.class);
		log.info("Input Path to the map-reduce job " + inputLoc1 + " and  "
				+ inputLoc2);
		log.info("Ouput Path to the map-reduce job " + outputLoc2);
		Path output1 = new Path(outputLoc2);
		FileSystem.get(conf2).delete(output1, true);
		FileOutputFormat.setOutputPath(job2, output1);
		job2.waitForCompletion(true);

		// Job 3
		conf3 = getConf();
		conf3.set("mapred.job.queue.name", "sree");
		Job job3 = new Job(conf3, "MapReduce : Final Join ");
		job3.setJarByClass(FinalDriver.class);
		FileInputFormat.addInputPath(job3, new Path(outputLoc2));
		job3.setMapperClass(FinalMapper.class);
		job3.setReducerClass(FinalReducer.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(NullWritable.class);
		job3.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job3, new Path(finalOutput));

		int status = (job3.waitForCompletion(true) == true) ? 0 : 1;
		return status;

	}

	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		int exit = ToolRunner.run(new IntmdteJoinDriver(), args);
		long end = System.currentTimeMillis();
		log.info("Total time taken to execute Join query is : " + (end - start)
				+ " millisecs");
		if (exit != 0) {
			log.error("Job failed with exit code " + exit);
			System.exit(exit);
		}
	}
}