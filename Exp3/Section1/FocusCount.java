package fbdp.Exp3.Section1;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FocusCount {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] the_line = value.toString().split("\\,");
            Text product = new Text(the_line[10]+","+the_line[1]);
            context.write(product, one);
        }
    }

    public static class IntSumCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }

        public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<String,Integer> map= new HashMap<>();
        private MultipleOutputs<Text, IntWritable> multipleOutputs;
        @Override
        protected void setup(Context context) {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            map.put(key.toString(), sum);
        }

        public void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException{
            List<Map.Entry<String,Integer>> list=new LinkedList<Map.Entry<String, Integer>>(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> n1, Map.Entry<String, Integer> n2) {
                    return (int)(n2.getValue() - n1.getValue());
                }
            });
            int max_output = 10;
            List<String> countedProvinces = new ArrayList<String>();
            List<Integer> eachOutputed = new ArrayList<Integer>();
            for(Map.Entry each:list){
                String[] theKey = each.getKey().toString().split(",");
                if(countedProvinces.contains(theKey[0])){
                    int index = countedProvinces.indexOf(theKey[0]);
                    int count = eachOutputed.get(index);
                    if(count<max_output){
                        eachOutputed.set(index, count+1);
                        multipleOutputs.write("EachProvince",theKey[1],each.getValue(),theKey[0] + "/" + theKey[0]);
                    }
                }else{
                    countedProvinces.add(theKey[0]);
                    eachOutputed.add(1);
                    multipleOutputs.write("EachProvince",theKey[1],each.getValue(),theKey[0] + "/" + theKey[0]);
                }
            }
            multipleOutputs.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job focus_count = Job.getInstance(conf, "Focus count");
        focus_count.setJarByClass(FocusCount.class);
        focus_count.setMapperClass(FocusCount.TokenizerMapper.class);
        focus_count.setMapOutputKeyClass(Text.class);
        focus_count.setMapOutputValueClass(IntWritable.class);
        focus_count.setCombinerClass(FocusCount.IntSumCombiner.class);
        focus_count.setReducerClass(FocusCount.IntSumReducer.class);
        focus_count.setOutputKeyClass(Text.class);
        focus_count.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(focus_count, new Path(args[0]));
        FileOutputFormat.setOutputPath(focus_count, new Path(args[1]));
        MultipleOutputs.addNamedOutput(focus_count,"EachProvince", TextOutputFormat.class,Text.class,Text.class);
        LazyOutputFormat.setOutputFormatClass(focus_count, TextOutputFormat.class);
        System.exit(focus_count.waitForCompletion(true) ? 0 : 1);
    }
}
