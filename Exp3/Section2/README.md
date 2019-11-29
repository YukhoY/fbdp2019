## 实验3 阶段2

本次实验在Ubuntu系统下进行，预先已完成了Hadoop的安装配置（伪分布式）。

安装Hive，首先下载编译好的包

```shell
$ wget https://mirrors.tuna.tsinghua.edu.cn/apache/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz
```

解压到跟Hadoop类似的位置，并且添加到环境变量，便于自己管理

```shell
$ tar -xzvf apache-hive-3.1.2-bin.tar.gz
$ export HIVE_HOME=~/apache-hive-3.1.2-bin
$ export PATH=$HIVE_HOME/bin:$PATH
```

再进行一些配置：

修改`hive-env.sh.template`的`HADOOP_HOME`，并**另存为**`hive-env.sh`：

![image-20191127113245473](./pics/image-20191127113245473.png)

创建`hive-site.xml`：

```xml
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
		<name>javax.jdo.option.ConnectionURL</name>
		<value>jdbc:derby:;databaseName=metastore_db;create=true</value>
	</property>
	<property>
		<name>hive.metastore.warehouse.dir</name>
		<value>/user/hive/warehouse</value>
	</property>
</configuration>
```

![image-20191127164549964](./pics/image-20191127164549964.png)

现在可以尝试启动Hive Shell了，进入Hive安装目录：

```shell
$ bin/hive
```

遇到了很多人遇到的坑：Hive和Hadoop的guava版本冲突，这套生态真的适配得不是很好……Apache要反思一下他们的项目管理了吧。

解决方法：分别打开`apache-hive-3.1.2-bin/lib`和`hadoop-3.2.1/share/hadoop/common/lib`，看到其中以`guava`开头的包，选择高版本的覆盖低版本的，不再赘述。

覆盖之后，再次运行以上命令，成功启动了：

![image-20191127151535272](./pics/image-20191127151535272.png)

但是一旦输入语句并运行，还会报错：

![image-20191127152829491](./pics/image-20191127152829491.png)

网上说原因是没有初始化metastore。其实启动hive的时候已经自动生成了一个，只不过那个没法用。

所以在启动Hive Shell之前，应当运行一次

```shell
$ bin/schematool -dbType derby -initSchema
```

完成初始化后便可进行shell命令了。如果格式化这一步报错，很可能是运行过一次shell之后，格式化目录已存在，因此需要把安装目录下的metastore_db目录删掉，再进行一次格式化。成功效果如图：

![image-20191127153232659](./pics/image-20191127153232659.png)

继续进行一些基本Hive操作：

```sql
hive> CREATE TABLE  pokes (foo INT, bar STRING);
hive> CREATE TABLE invites (foo INT, bar STRING) PARTITIONED BY (ds STRING);
hive> CREATE TABLE Shakespeare (freq int, word string) row format delimited fields terminated by  '\t' stored as textfile;
hive> DESCRIBE invites;
hive> DROP TABLE pokes;
hive> DROP TABLE invites;
hive> DROP TABLE Shakespeare;
```

走一遍创建删除流程，效果如下：

![image-20191127154029106](./pics/image-20191127154029106.png)

接下来就可以开始做真正实验啦！

为了方便，我们将数据表格放到路径`~/fbdp_data`文件夹下。

先根据给出的表结构，创建一个数据表：

```sql
hive> CREATE TABLE  million_user_log (user_id INT, item_id INT, cat_id INT, merchant_id INT, brand_id INT, month INT, day INT, action INT, age_range INT, gender INT, province STRING) row format delimited fields terminated by ',';
```

将文件插入数据表中：

```sql
hive> LOAD DATA LOCAL INPATH '/home/yukho/fbdp_data/million_user_log.csv' INTO TABLE million_user_log;
```

取两行出来看看效果：

```sql
hive> select * from million_user_log limit 0,2;
```

![image-20191127182556939](./pics/image-20191127182556939.png)

装载成功！于是我们就可以按照类似于SQL的方式提取数据出来了。

首先，计算这一百条记录里面，有多少人购买了商品，涉及选取`action=2`，且需要对`user_id`要是`distinct`的。

```sql
hive> SELECT count(DISTINCT user_id) FROM million_user_log WHERE action=2;
```

> 截取输出如下：
>
> OK
> 37202
> Time taken: 17.013 seconds, Fetched: 1 row(s)

![](./pics/image-20191127183946832.png)

计算男女买家购买的比例，那就在上一条的基础上，按gender列分组，并且按user_id DISTINCT，得到下面代码：

```sql
hive> SELECT gender,count(DISTINCT user_id) FROM million_user_log WHERE action=2 GROUP BY gender;
```

![image-20191129200515592](./pics/image-20191129200515592.png)

容易得到男女购买者的比例为22413:22477，接近1:1。当然也可以计算出男女买家分别占总体的比例：

```sql
hive> SELECT (gender,count(DISTINCT user_id)/(SELECT count(DISTINCT user_id) FROM million_user_log WHERE action=2)) FROM million_user_log WHERE action=2 GROUP BY gender;
```

![image-20191127190445624](./pics/image-20191127190445624.png)

输出的结果有点不科学，但逻辑上似乎没有问题，判断原因正如同学在群里所说，同一个用户id会对应不同的性别导致的。于是想到一个办法，先新建一个表筛选出distinct user_id，再分组会怎么样？（这个后续再实现，暂时没成功）

```sql
// TODO: Clean up data noise and redo the experiment.
```

最后查询双十一当天浏览次数前十名品牌：

```sql
hive> SELECT brand_id,count(brand_id) number FROM million_user_log WHERE action=0 GROUP BY brand_id ORDER BY number DESC LIMIT 10;
```

![image-20191129201716382](./pics/image-20191129201716382.png)

这里的结果是考虑的是总浏览人次，而如果想要计算的是浏览的人数，即不计算单人重复浏览，则应修改如下：

```sql
hive> SELECT brand_id,count(DISTINCT user_id) number FROM million_user_log WHERE action=0 GROUP BY brand_id ORDER BY number DESC LIMIT 10;
```

![image-20191129201945548](./pics/image-20191129201945548.png)

则阶段二到此结束。