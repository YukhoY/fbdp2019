---
typora-root-url: /media/yukho/Data/Documents/GitHub/fbdp2019/Exp3/Section1/pics
typora-copy-images-to: pics
---

## 实验3 阶段1

原思路是先将不同省份的数据提取出成单独的文件，再进行后续计算处理，已经写好了按省份分割的代码（SplitProvince.java），后来觉得分两次任务有点麻烦，于是改成了下面的方法，但原代码也一并提交在本目录下：

### **统计关注量**：

本阶段报告着重于展示本程序设计的思路和部分结果，完整结果查看方式将在后文提到。

整体框架与WordCount程序相近，作出了许多改进以适应本需求。

#### Map设计：

将数据集按行读入后，按逗号分割每行，取出省份和产品id，合并作为Map的Key，Value为1。

```java
public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] the_line = value.toString().split("\\,");
            Text product = new Text(the_line[10]+","+the_line[1]);
            context.write(product, one);
        }
```

##### Combiner设计

与WordCount的Combiner几乎一致，仅将每个键值对先行作部分合并。

#### Reduce设计：

为所有reducer定义一个共用的Map对象来储存Reduce结果而不是直接输出，在所有reduce执行完后的cleanup阶段再进行统一整理输出，同时为每个Reducer开启一个共用的输出对象，来进行多目录输出。

```java
private Map<String,Integer> map= new HashMap<>();
private MultipleOutputs<Text, IntWritable> multipleOutputs;
```

在setup阶段设置对输出结构进行设置，来进行多目录输出：

```java
protected void setup(Context context) {
            multipleOutputs = new MultipleOutputs<>(context);
       }
```

在reduce阶段，先计算当前key下value的和，然后输出到前面定义的Map结构中。

```java
public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            map.put(key.toString(), sum);
        }
```

在cleanup阶段，首先将刚才储存下来的reduce结果进行排序后，按降序遍历每一个结果，遍历途中记录当前省份已经输出过的记录的个数，从储存的Key中分割开先前在Map阶段合并的省份与产品id，如果当前记录的省份已经输出的数量已经达到10个，则不再输出当前省份的结果，否则输出key该产品id、value为热门关注统计量，到以省份命名的文件夹下的{{省份名}}-m-0000文件。

```java
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
```

#### Main函数设计：

此处主要设置map、reduce及它们对应的输入输出类，均为<Text, IntWritable>。设置任务输出类为多文件输出MultipleOutputs，并设置其输出格式为<Text，Text>。且取消原本的默认输出，否则会生成一个空文件。

```java
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
```

所有输出结果均在output_focus文件夹下，以省份为名的子文件夹里的{{省份名}}-m-0000文件中。

此处给出部分样例，完整结果在本仓库目录的相同文件夹下查看：

![image-20191129205846522](/../../../../../../../../../pics/image-20191129205846522.png)

![image-20191129205956560](/../../../../../../../../../pics/image-20191129205956560.png)

![image-20191129210015279](/../../../../../../../../../pics/image-20191129210015279.png)

### **统计购买量**：

与上一程序相比，修改仅限对将Map阶段的输出添加action为2的判断条件，其余基本不变：

```java
public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] the_line = value.toString().split("\\,");
            if(Integer.parseInt(the_line[7]) == 2){
                Text product = new Text(the_line[10]+","+the_line[1]);
                context.write(product, one);
            }
        }
```

输出均在output_buy文件夹下，以省份为名的子文件夹里的{{省份名}}-m-0000文件中。

此处同样给出部分样例：

![image-20191129210437105](/../../../../../../../../../pics/image-20191129210437105.png)

![image-20191129210453522](/../../../../../../../../../pics/image-20191129210453522.png)

![image-20191129210507974](/../../../../../../../../../pics/image-20191129210507974.png)

