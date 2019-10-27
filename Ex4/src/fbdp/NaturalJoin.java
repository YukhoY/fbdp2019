package fbdp;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * ��Ȼ���Ӳ���,������col�Ͻ�������
 * @author KING
 *
 */
public class NaturalJoin {
	public static class NaturalJoinMap extends Mapper<Text, BytesWritable, Text, Text>{
		private int col;
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			col = context.getConfiguration().getInt("col", 0);
		}
		@Override
		public void map(Text relationName, BytesWritable content, Context context)throws 
		IOException, InterruptedException{
			String[] records = new String(content.getBytes(),"UTF-8").split("\\r\\n");
			for(int i = 0; i < records.length; i++){
				if (relationName.toString().equals("Rb.txt")) {
					RelationB record = new RelationB(records[i]);
					context.write(new Text(record.getCol(col)),
							new Text(relationName.toString() + " " + record.getValueExcept(col)));
				}
				else {
					RelationA record = new RelationA(records[i]);
					context.write(new Text(record.getCol(col)),
							new Text(relationName.toString() + " " + record.getValueExcept(col)));
				}
			}
		}
	}
	
	public static class NaturalJoinReduce extends Reducer<Text,Text,Text,NullWritable>{
		private String relationNameA;
		protected void setup(Context context) throws IOException,InterruptedException{
			relationNameA = context.getConfiguration().get("relationNameA");
		}
		public void reduce(Text key, Iterable<Text> value, Context context)throws 
		IOException,InterruptedException{
			ArrayList<Text> setR = new ArrayList<Text>();
			ArrayList<Text> setS = new ArrayList<Text>();
			String[] setA = new String[3];
			String[] setB = new String[2];
			//������Դ��Ϊ����Ȼ�����ѿ����˻�
			for(Text val : value){
				String[] recordInfo = val.toString().split(" ");
				if(recordInfo[0].equalsIgnoreCase(relationNameA))
					setA = recordInfo[1].split(",");
					//setR.add(new Text(recordInfo[1]));
				else
					//setS.add(new Text(recordInfo[1]));
					setB = recordInfo[1].split(",");
			}
			Text t = new Text(key.toString() + "," + setA[0] + "," + setA[1] + "," + setB[0] + "," + setA[2] + "," + setB[1]);
			context.write(t, NullWritable.get());
//			for(int i = 0; i < setR.size(); i++){
//				for(int j = 0; j < setS.size(); j++){
//					Text t = new Text(key.toString() + "," + setR.get(i).toString() + "," + setS.get(j).toString());
//					context.write(t, NullWritable.get());
//				}
//			}
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		Job naturalJoinJob = Job.getInstance(conf, "NaturalJoin");
		naturalJoinJob.setJobName("naturalJoinJob");
		naturalJoinJob.setJarByClass(NaturalJoin.class);
		naturalJoinJob.getConfiguration().setInt("col", Integer.parseInt(args[2]));
		naturalJoinJob.getConfiguration().set("relationNameA", args[3]);
		
		naturalJoinJob.setMapperClass(NaturalJoinMap.class);
		naturalJoinJob.setMapOutputKeyClass(Text.class);
		naturalJoinJob.setMapOutputValueClass(Text.class);

		naturalJoinJob.setReducerClass(NaturalJoinReduce.class);
		naturalJoinJob.setOutputKeyClass(Text.class);
		naturalJoinJob.setOutputValueClass(NullWritable.class);

		naturalJoinJob.setInputFormatClass(WholeFileInputFormat.class);
		naturalJoinJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(naturalJoinJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(naturalJoinJob, new Path(args[1]));
		
		naturalJoinJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}
