import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Autocomplete {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Configuration config = context.getConfiguration();
			String line = value.toString();
			String parts[] = line.toLowerCase().split("\t");

			for (int i = 0; i < parts.length; i++) {
				if (parts[i].length() > 2) {
					for (int j = 2; j <= parts[i].length() - 1; j++) {
						context.write(new Text(parts[i].substring(0, j)),
								new Text(parts[i] + ":"+parts[1]));
					}
				} else
					context.write(new Text(parts[i]), new Text(parts[i] + ":"+parts[1]));
			}
		}
	}

	public static class IntSumReducer extends
			TableReducer<Text, Text, ImmutableBytesWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			Configuration config = context.getConfiguration();

			TreeSet<WordSort> sortedSuffixes = new TreeSet<WordSort>();
			for (Text val : values) {
				sortedSuffixes.add(new WordSort(val.toString()));
			}
			int top5=1;
			for (WordSort element : sortedSuffixes) {
				if(top5<=5){
					Put put = new Put(Bytes.toBytes(key.toString()));
					put.add(Bytes.toBytes("suffix"),
							Bytes.toBytes(element.suffix),
							Bytes.toBytes(element.count));
					context.write(null, put);
					top5++;
				}
				else{
					break;
				}
			}
		}
	}

	private static class WordSort implements Comparable<WordSort> {

		String suffix = null;
		Integer count = 0;

		WordSort(String wordCount) {
			String[] tokens = wordCount.split(":");
			suffix = tokens[0];
			count = Integer.parseInt(tokens[1]);
		}

		@Override
		public int compareTo(WordSort o) {
			return o.count.compareTo(this.count);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "Autocomplete");
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		
		job.setJarByClass(Autocomplete.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ImmutableBytesWritable.class);

		TableMapReduceUtil.initTableReducerJob("autocomplete", IntSumReducer.class,
				job);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
