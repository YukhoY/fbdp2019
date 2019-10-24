## 作业4



1. 给出矩阵乘法的MapReduce实现，以M_3_4和N_4_2作为输入进行测试。

2. 给出关系代数的选择、投影、并集、交集、差集及自然连接的MapReduce实现。测试集如下：

- 关系Ra：(id, name, age, weight)
- 关系Rb：(id, gender, height)

​	2.1 在Ra.txt上选择age=18的记录；在Ra.txt上选择age<18的记录

​	2.2 在Ra.txt上对属性name进行投影

​	2.3 求Ra1和Ra2的并集

​	2.4 求Ra1和Ra2的交集

​	2.5 求Ra2 - Ra1

​	2.6 Ra和Rb在属性id上进行自然连接，要求最后的输出格式为(id, name, age, gender, weight, height)

注意：上述实验，均需给出源代码和运行结果截屏。本次作业不需要详细报告，但要附带程序readme说明。可以将程序代码、截图和readme上传至github，给出地址。

提示：原书代码仅供参考，有坑！！！（[点此下载](http://pasa-bigdata.nju.edu.cn/booksourcecode/booksourcecode.zip)）

**魔改原书代码**

包含的修改如下：

1. 添加了包声明 package fbdp
2. 修改了Configuration方式
3. 修改了Job定义方法
4. 为main入口添加静态标识
5. 删除一些无用或错误配置
6. 修改了一些内部逻辑

**代码已编译打包为Ex4.jar**，结合输入文件（上传到hdfs文件系统中）使用

矩阵乘法，用法:

> bin/hadoop jar <PathToJar> fbdp.MatrixMultiply <InputPathToM1> <InputPathToM2> <OutputPath>
>
> > Input输入：两个矩阵，必须可以相乘
> >
> > 输出矩阵乘法结果中，行、列与矩阵值的对应值

```shell
$ bin/hadoop jar Ex4.jar fbdp.MatrixMultiply tests/input/M_3_4 tests/input/N_4_2 mm_output/
$ bin/hdfs dfs -cat mm_output/*
```



关系代数选择，用法:

> bin/hadoop jar <PathToJar> fbdp.Selection <InputPath> <OutputPath> <Col> <Value> <Mode>
>
> > Input输入：按RelationA关系定义的数据文件
> >
> > Col为整数，与各列对应关系如下（仅适用于RelationA）：
> >
> > 0：id序号
> >
> > 1：name姓名
> >
> > 2：age年龄
> >
> > 3：weight体重
> >
> > Value指所选的Col对应筛选条件的指定值
> >
> > Mode指筛选模式：
> >
> > bigger：大于指定值
> >
> > small：小于指定值
> >
> > equal：等于指定值
> >
> > 输出满足筛选条件的所有行

```shell
$ bin/hadoop jar Ex4.jar fbdp.Selection tests/input/Ra.txt selection_output/ 2 18 equal
$ bin/hdfs dfs -cat selection_output/*
$ bin/hadoop jar Ex4.jar fbdp.Selection tests/input/Ra.txt selection_output1/ 2 18 smaller
$ bin/hdfs dfs -cat selection_output1/*
```

关系代数投影，用法:

> bin/hadoop jar <PathToJar> fbdp.Projection <InputPath> <OutputPath> <Col>
>
> > Input输入：按RelationA关系定义的数据文件
> >
> > Col为整数，与各列对应关系如下（仅适用于RelationA）：
> >
> > 0：id序号
> >
> > 1：name姓名
> >
> > 2：age年龄
> >
> > 3：weight体重
> >
> > 输出所选属性的投影

```shell
$ bin/hadoop jar Ex4.jar fbdp.Projection tests/input/Ra.txt projection_output/ 1
$ bin/hdfs dfs -cat projection_output/*
```

关系投影并集，用法：

> bin/hadoop jar <PathToJar> fbdp.Union <InputPath> <OutputPath>
>
> > Input输入：含有两个关系数据文件的文件夹路径
> >
> > 输出交集

```shell
$ bin/hadoop jar Ex4.jar fbdp.Union tests/union_input/ union_output/
$ bin/hdfs dfs -cat union_output/*
```

关系投影交集，用法：

> bin/hadoop jar <PathToJar> fbdp.Intersection <InputPath> <OutputPath>
>
> > Input输入：含有两个关系数据文件的文件夹路径
> >
> > 输出交集

```shell
$ bin/hadoop jar Ex4.jar fbdp.Intersection tests/inter_input/ inter_output/
$ bin/hdfs dfs -cat inter_output/*
```

关系投影差集，用法：

> bin/hadoop jar <PathToJar> fbdp.Difference <InputPath> <OutputPath> <FilePath>
>
> > Input输入：含有两个关系数据文件的文件夹路径
> >
> > FilePath为被减文件的名字
> >
> > 输出差集

```shell
$ bin/hadoop jar Ex4.jar fbdp.Difference tests/diff_input/ diff_output/ Ra1.txt
$ bin/hdfs dfs -cat diff_output/*
```

关系投影自然连接 ，用法：

> bin/hadoop jar <PathToJar> fbdp.NaturalJoin <InputPath> <OutputPath> <Col> <FilePath>
>
> > Input输入：含有两个关系数据文件的文件夹路径
> >
> > Col为对齐列
> >
> > FilePath为其中一个输入文件名，用于指定前后顺序
> >
> > 输出自然连接集，列顺序根据题目输出要求做了预定义

```shell
$ bin/hadoop jar Ex4.jar fbdp.NaturalJoin tests/join_input/ join_output/ 0 Ra.txt
$ bin/hdfs dfs -cat join_output/*
```

