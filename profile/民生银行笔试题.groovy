// 以下关于Flume, 说法错误的是( D )
A: Flume 以agent 为最小的独立运行单位, 一个agent 就是一个JVM。单agent 由Source、Sink 和Channel 三大组件构成
B: Flume 的数据流由事件 (Event) 贯穿始终, 事件是 Flume 的基本数据单位
C: Flume 三种级别得可靠性保障, 从强到弱依次为: end-to-end、Store on failure、Besteffort
D: Channel 中 filechannel 可将数据持久化到本地磁盘, 但配置较为麻烦, 需要配置数据目录和checkpoint目录, 
   不同的file channel可以配置同一个checkpoint 目录


// 查看 kafka 某 topic 的 partition 详细信息时，使用如下哪个命令( C )
A: bin/kafka-topics.sh --create
B: bin/kafka-topics.sh --list
C: bin/kafka-topics.sh --describe
D: bin/kafka-topics.sh --delete


// HIVE 中表的默认存储格式为( A )
A: TextFile
B: Avro
C: SequenceFile
D: RCFile


// HDFS 中的block 默认保存几份( C )
A: 1
B: 2
C: 3
D: none of the above


// 下面哪个进程负责HDFS 数据存储( C )
A: NameNode
B: JobTracker
C: DataNode
D: SecondaryNameNode


// HBase 依靠( A )存储底层数据
A: HDFS
B: memory
C: mapreduce
D: hadoop


// 下面哪条命令可以把f1.txt 复制为f2.txt?( C )
A: cp f1.txt | f2.txt
B: cat f1.txt | f2.txt
C: cat f1.txt > f2.txt
D: copy f1.txt | f2.txt


// 怎样更改一个文件的权限设置( B )
A: attrib
B: chmod
C: change
D: file


// 下列哪些选项可以查看hdfs 文件系统指定目录下文件的命令( D )
A: hadoop -ls /home
B: hadoop -fs -ls /home
C: hadoop -fs ls /home
D: hadoop fs -ls /home


// 如果有多个Kafka 程序同时消费一个topic, 如何保证取到不同的事件( B )
A: 使用相同的client.id
B: 使用相同的group.id
C: 使用相同的zookeeper
D: 使用相同的bootstrap-server


// 大表1000 万条数据, 小表1000 条数据，为提高查询效率两行表关联时通常做法是( B )
A: 大表在前
B: 大表在后
C: 小表子查询
D: 先处理成一张表再查询


// HDFS 的设计中没有考虑以下哪个特性( D )
A: 超大文件
B: 流式的数据访问
C: 高吞吐
D: 低数据延迟


// 下列对数据库事务的描述正确的是( A,C,D )
A: 一致性
B: 独立性
C: 持久性
D: 原子性


// 下面哪个不负责HDFS 的数据存储( B,C,D )
A: DataNode
B: NameNode
C: secondNameNode
D: DfsClient


// 在SQL 中以下哪些方式可以用来对数据排序( order by, row_number, rank )
partition by是分区


// Hadoop 默认调度器策略为FIFO( 错误 )
apache hadoop默认调度器是容量调度器


// kafka 的数据是存储在内存中的( 正确 )
个人思考应该是不完全正确