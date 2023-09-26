// 1. MPP( 大规模并行处理 )数据库
    clickhouse、doris、greenplum

// 2. clickhouse数据量
    百万日活, 每天将近20GB, 保存一个月 600GB, 保守一些1TB、保存半年 3600GB, 保守一些4TB

// 3. clickhouse节点数, 或者说部署几台？
    1台 -> 数据量小, 单台无可厚非, 但是听起来有点low

    最好说3台 -> 3台的话可以用分布式表、副本表

// 4. clickhouse部署
    clickhouse不能与yarn或者hdfs部署在一块, 由于它的存储不依赖于外部, 查询的时候会尽可能占用
    机器资源, 那么如果和hadoop部署在一块儿, 这是会受影响的.

// 5. clickhouse的数据一致性
    flink程序故障, checkpoint恢复的话, 会出现重复写入, 只能保证至少一次
    咱们用的replacingMergeTree, 会按照主键去重, 但是呢, 分片没有合并前, 还是存在重复, 可以直接在查询的时候加上关键字 final
    不管你有没有合并, 那我在查的时候你必须给我合并

5.1 普通查询
    select 
        * 
    from a 
    WHERE dt = '2014-03-17' 
    settings max_threads = 2;

    5.2 final 查询
    select 
        * 
    from a final 
    WHERE dt = '2014-03-17' 
// 设置查询的线程数, 提高查询速度
    settings max_final_threads = 2;

// 6. clickhouse优缺点
6.1 就是快, 一亿行180+字段在单机版clickhouse聚合统计分析中就能毫秒级返回

所有mpp数据库中, 目前clickhouse是最快的

6.2 列式、列存、向量化、可以自定义udf、支持sql语法这是很友好的

6.3 单查询你尽可能占用资源, 你要执行多个查询呢？所以并发弱, 默认单台包含读和写请求, 
总共支持100个请求每秒 -> max_concurrent_queries -> 100/s

咱们3台的话, 调到300就差不多了

当然你节点数够多, 你也可以调的更多一些

6.4 join性能差, join实现比较奇葩, 多表join的时候, 它类似于一定会把右表广播到左表所在的节点中, 然后存到它的内存中, 进行本地join
那么如果右表是大表的话就崩了, 所以一定得小表放在右边

因为咱们在clickhouse中的存的是大宽表, 所以不会遇到这个join的问题

6.5 不像doris, 扩缩容很友好, clickhouse假设你增加结点那么你就需要手动的均衡各结点的数据, 容易出现元数据错误问题

// 7. 资源优化(内存、 CPU)
    kafka启动内存咱们由1GB改成了10GB, 但是clickhouse是没有启动内存的这一说的, 他只会说你聚合查询分析的时候
    它会尽可能的占用你节点的资源, max_memory_usage = 100GB(咱们是128GB内存的服务器); 不是说你一启动就固定占用
    多少内存, 不是, 咱们限制的是它的天花板

    你每秒2万条写入速度, 对clickhouse压力也大, 咱们使用开5秒窗口攒批写入, 5s写一次能减轻clickhouse的压力
