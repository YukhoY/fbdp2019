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

public class SalesCount {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] the_line = value.toString().split("\\,");
            if(Integer.parseInt(the_line[7]) == 2){
                Text product = new Text(the_line[10]+","+the_line[1]);
                context.write(product, one);
            }
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
        private Map<String,Integer> map= new HashMap<String, Integer>();
        private IntWritable result = new IntWritable();
        private MultipleOutputs<Text, IntWritable> multipleOutputs;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
        }

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            String[] pro_name = key.toString().split(",");
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
        Job salesCount = Job.getInstance(conf, "Sales count");
        salesCount.setJarByClass(SalesCount.class);
        salesCount.setMapperClass(SalesCount.TokenizerMapper.class);
        salesCount.setMapOutputKeyClass(Text.class);
        salesCount.setMapOutputValueClass(IntWritable.class);
        salesCount.setCombinerClass(SalesCount.IntSumCombiner.class);
        salesCount.setReducerClass(SalesCount.IntSumReducer.class);
        salesCount.setOutputKeyClass(Text.class);
        salesCount.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(salesCount, new Path(args[0]));
        FileOutputFormat.setOutputPath(salesCount, new Path(args[1]));
        MultipleOutputs.addNamedOutput(salesCount,"EachProvince", TextOutputFormat.class,Text.class,Text.class);
        LazyOutputFormat.setOutputFormatClass(salesCount, TextOutputFormat.class);
        System.exit(salesCount.waitForCompletion(true) ? 0 : 1);
    }
}
