package fbdp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


/**
 * �󽻼�������ÿ��record����(record,1)��reduce
 * ʱֵΪ2�ŷ����record
 * @author KING
 *
 */
public class Union {
	public static class UnionMap extends Mapper<LongWritable, Text, RelationA, IntWritable>{
		private IntWritable one = new IntWritable(1);
		@Override
		public void map(LongWritable offSet, Text line, Context context)throws 
		IOException, InterruptedException{
			RelationA record = new RelationA(line.toString());
			context.write(record, one);
		}
	}
	public static class UnionReduce extends Reducer<RelationA, IntWritable, RelationA, NullWritable>{
		@Override
		public void reduce(RelationA key, Iterable<IntWritable> value, Context context) throws 
		IOException,InterruptedException{
			//int sum = 0;
			for(IntWritable val : value){
				//sum += val.get();
				if (val.get()==1){
					context.write(key, NullWritable.get());
					break;
				}
			}
			//if(sum == 2)
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		Job UnionJob = Job.getInstance(conf, "Union");
		UnionJob.setJobName("UnionJob");
		UnionJob.setJarByClass(Union.class);
	
		UnionJob.setMapperClass(UnionMap.class);
		UnionJob.setMapOutputKeyClass(RelationA.class);
		UnionJob.setMapOutputValueClass(IntWritable.class);

		UnionJob.setReducerClass(UnionReduce.class);
		UnionJob.setOutputKeyClass(RelationA.class);
		UnionJob.setOutputValueClass(NullWritable.class);

		UnionJob.setInputFormatClass(TextInputFormat.class);
		UnionJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(UnionJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(UnionJob, new Path(args[1]));
		
		UnionJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}
