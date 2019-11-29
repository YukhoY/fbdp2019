package fbdp.Exp3.Section1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SplitProvince {
    public static class SplitProvinceMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] the_line = value.toString().split(",");
            Text province = new Text(the_line[10]);

            context.write(province, value);
        }
    }

    public static class SplitProvinceReducer
            extends Reducer<Text, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> multipleOutputs;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<NullWritable,Text>(context);
        }

        // private Map<String,Integer> map=new HashMap<String, Integer>();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for(Text value:values) {
               // context.write(NullWritable.get(), value);,
                multipleOutputs.write("EachProvince",NullWritable.get(),value,key.toString() + "/" + key.toString());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job split_province = Job.getInstance(conf, "split_province");
        split_province.setJarByClass(SplitProvince.class);
        split_province.setMapperClass(SplitProvince.SplitProvinceMapper.class);
        split_province.setMapOutputKeyClass(Text.class);
        split_province.setMapOutputValueClass(Text.class);
        split_province.setCombinerClass(SplitProvince.SplitProvinceReducer.class);
        split_province.setReducerClass(SplitProvince.SplitProvinceReducer.class);
        split_province.setOutputKeyClass(NullWritable.class);
        split_province.setOutputValueClass(Text.class);
        MultipleOutputs.addNamedOutput(split_province,"EachProvince", TextOutputFormat.class,NullWritable.class,Text.class);
        //split_province.setInputFormatClass(TextInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(split_province, TextOutputFormat.class);
        FileInputFormat.addInputPath(split_province,new Path((args[0])));
        FileOutputFormat.setOutputPath(split_province,new Path(args[1]));
        System.exit(split_province.waitForCompletion(true) ? 0 : 1);
    }
}
