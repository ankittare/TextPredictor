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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LanguageModel {

    /**
     * Mapper Class
     * 
     * @author Ankit
     *
     */
    public static class TokenizerMapper extends
	    Mapper<Object, Text, Text, Text> {
	private String newValue = new String();

	public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {

	    Configuration config = context.getConfiguration();
	    String threshold = config.get("threshold");
	    String tokens[] = value.toString().split("\t");
	    String keyParts[] = tokens[0].split("\\s+");
	    /**
	     * writing one grams if they have count greater than one
	     */
	    if (keyParts.length == 1) {
		if (Integer.parseInt(tokens[1]) > Integer.parseInt(threshold)) {
		    context.write(new Text(tokens[0]), new Text(tokens[1]));
		}
	    } else {
		/**
		 * if the grams is more than one, then extracting the base case
		 * and sending it to reducer
		 */
		if (Integer.parseInt(tokens[1]) > Integer.parseInt(threshold)) {
		    context.write(new Text(tokens[0]), new Text(tokens[1]));
		    String baseCase = "";
		    for (int i = 0; i < keyParts.length - 1; i++) {
			baseCase = baseCase + " " + keyParts;
		    }
		    baseCase = baseCase.substring(0, baseCase.length() - 1);
		    newValue = keyParts[keyParts.length - 1] + " " + tokens[1];
		    context.write(new Text(baseCase), new Text(newValue));
		}
	    }
	}
    }

    /**
     * Reducer Class
     * 
     * @author Ankit
     *
     */
    public static class IntSumReducer extends
	    TableReducer<Text, Text, ImmutableBytesWritable> {
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
	    double baseCount = 0;
	    HashMap<String, Double> topN = new HashMap<String, Double>();
	    Configuration config = context.getConfiguration();
	    String threshold = config.get("topN");

	    TreeSet<Corpus> sortedPhrases = new TreeSet<Corpus>();

	    for (Text temp : values) {
		sortedPhrases.add(new Corpus(key.toString(), temp.toString()));
	    }
	    for (Corpus element : sortedPhrases) {
		/**
		 * identifying a base case
		 */
		if (!element.toString().contains(":")) {
		    baseCount = Double.parseDouble(element.toString());
		    break;
		}
	    }

	    int k = 1;
	    for (Corpus element : sortedPhrases) {
		if (k < Integer.parseInt(threshold)
			&& element.toString().contains(":") == true) {
		    /**
		     * Storing the key and the respective probablity for all
		     * grams
		     */
		    topN.put(
			    element.toString().split(":")[0].toString(),
			    ((Double.parseDouble(element.toString().split(":")[1]
				    .toString())) / baseCount));
		    k++;
		}
		if (k == 6)
		    break;
	    }

	    /**
	     * iterate over the map and put the entries into hbase
	     */
	    Iterator iterator = topN.entrySet().iterator();
	    int m = 1;
	    while (iterator.hasNext()) {
		if (m < 6) {
		    m++;
		    Map.Entry pair = (Map.Entry) iterator.next();
		    Put put = new Put(Bytes.toBytes(key.toString()));
		    /**
		     * HBase with column family Phrase
		     */
		    put.add(Bytes.toBytes("phrase"),
			    Bytes.toBytes(pair.getKey().toString()),
			    Bytes.toBytes(pair.getValue().toString()));
		    context.write(null, put);
		} else {
		    break;
		}
	    }
	}
    }

    /**
     * Corpus class for sorting the input to the reducer
     * 
     * @author Ankit
     *
     */
    private static class Corpus implements Comparable<Corpus> {

	String returnLine = null;
	String phrase = null;
	int count = 0;

	Corpus(String key, String value) {
	    String tokens[] = value.split("\\s+");
	    if (tokens.length == 1) {
		phrase = "";
		count = Integer.parseInt(tokens[0]);
		returnLine = Integer.toString(count);
	    } else {
		/**
		 * putting a colon to idenfity a base case
		 */
		phrase = tokens[0];
		count = Integer.parseInt(tokens[1]);
		returnLine = phrase + ":" + count;
	    }
	}

	public String toString() {
	    return returnLine;
	}

	@Override
	public int compareTo(Corpus o) {
	    if (this.count - o.count == 0) {
		return this.phrase.compareTo(o.phrase);
	    } else if (this.count - o.count < 0)
		return 1;
	    else
		return -1;
	}
    }

    public static void main(String[] args) throws Exception {

	Configuration conf = HBaseConfiguration.create();
	Job job = Job.getInstance(conf, "LanguageModel");
	String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();

	job.getConfiguration().set("threshold", otherArgs[2]);

	job.getConfiguration().set("topN", otherArgs[3]);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	job.setJarByClass(LanguageModel.class);
	job.setMapperClass(TokenizerMapper.class);
	job.setReducerClass(IntSumReducer.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(ImmutableBytesWritable.class);

	TableMapReduceUtil.initTableReducerJob("predict", IntSumReducer.class,
		job);

	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
