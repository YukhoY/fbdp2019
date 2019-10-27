package fbdp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 获得列号为id的列上所有值为value的元组
 * @author KING
 *
 */
public class Selection {
	public static class SelectionMap extends Mapper<LongWritable, Text, RelationA, NullWritable>{
		private int id;
		private String value;
		private String mode;
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			id = context.getConfiguration().getInt("col", 0);
			value = context.getConfiguration().get("value");
			mode = context.getConfiguration().get("mode");
		}

		public void map(LongWritable offSet, Text line, Context context)throws
		IOException, InterruptedException{
			RelationA record = new RelationA(line.toString());
			if(record.isCondition(id, value, mode))
				context.write(record, NullWritable.get());
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		Job selectionJob = Job.getInstance(conf, "Selection");
		selectionJob.setJarByClass(Selection.class);
		selectionJob.getConfiguration().setInt("col", Integer.parseInt(args[2]));
		selectionJob.getConfiguration().set("value", args[3]);
		selectionJob.getConfiguration().set("mode", args[4]);
		
		selectionJob.setMapperClass(SelectionMap.class);
		selectionJob.setMapOutputKeyClass(RelationA.class);
		selectionJob.setMapOutputValueClass(NullWritable.class);

		selectionJob.setNumReduceTasks(0);

		//selectionJob.setInputFormatClass(FileInputFormat.class);
		//selectionJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(selectionJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(selectionJob, new Path(args[1]));
		
		selectionJob.waitForCompletion(true);
	}
}
