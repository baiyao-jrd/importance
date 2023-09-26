// 1. flink反压
checkpoint超时、失败, 或者说状态变大了, 消费速率变慢kafka数据积压了, 就能判定是反压出现了, 另外
你在flink webui界面就能看到出现了灰色节点

那怎么定位哪个算子反压了呢? 可以通过webui, 另外就是通过它自己有的metrics指标来判断反压节点

1.1 webui定位
有两种情况
(1) 常见的一种, 每个算子有输入输出缓冲区, 假设source -> A -> B -> C, C算子有时输入和输出缓冲区, 
它处理能力不行, 输入缓冲区就会缓存大量数据, 满了之后, 会造成B算子输出缓冲区塞满数据, 就这样链式的
积压数据最终会使source算子没法消费kafka数据, 或者说消费kafka数据速率很慢很慢, 这种情况呢, C算子是
罪魁祸首, 但是webui的backpressure会显示ok, 但它的上游都会会显示high

(2) 少见的一种, source -> A -> B -> C, 假设C这个算子是一斤多出的算子, 那么假设你在实现flatmapFunction里面的
方法是用循环将流入的一个数据转换往下游发送了大量数据产生了数据膨胀, 这时候这个算子就会在backpressure这边显示为high, 
这将导致其后面的算子也显示为high

简单总结:
a. 上游全是high, 找第一个ok节点
b. 上游全是ok, 找第一个high节点

1.2 metrics定位
原理很简单, 算子有输入和输出缓冲区, 那么你通过webui的metric图可以查看该算子是否是被反压,
某算子的buffers.inPoolUsage输入缓冲区使用率与buffers.outpoolUsage输出缓冲区使用率, 若
buffers.outpoolUsage为1就可以确定当前节点受到了反压, 总结几种情况

(1) inPoolUsage高, outPoolUsage低 -> 祸首节点 -> 符合: 上游全是high, 找第一个ok节点
(2) inPoolUsage低, outPoolUsage高 -> 祸首节点, 数据膨胀 -> 符合: 上游全是ok, 找第一个high节点
(3) inPoolUsage低, outPoolUsage低 -> 俩都低, 说明你没反压
(4) inPoolUsage高, outPoolUsage高 -> 俩都高, 说明你是被反压的, 你不是反压根源

1.3 注意算子链禁用, 否则不方便定位, 你webui一个方框代表一个算子的话, 才好使用咱们上面的定位方法
由于flink优化会产生operator chain, 这里要找反压算子就需要程序里面env.disableOperatorChaining()将算子链先禁用

1.4 解决
(1) 先看是不是倾斜了, 是的话就按照倾斜的解决方法来对应解决
(2) 若不是发生了数据倾斜的话, 就得按照个人经验来判断一下代码写的是不是低性能
(3) 如果说未倾斜并且代码也没问题的话, 就得借助工具 - 火焰图分析了
    程序里面需要设置new Configuration().set("rest.flamegraph.enabled", true)来开启就行了,
    当然也可以在flink run -D参数设为true 的时候来指定

    打开后, 点算子webui里面可以看到flamegraph火焰图, 横向看是某个类的某个方法执行的时长, 纵向看是
    里面的先调的哪个类的哪个方法之后后调的哪个类的哪个方法, 算是调用顺序链, 那么最终的执行方法肯定是最顶端
    的方法, 那么找最上面的大平顶, 也就说明这个方法运行时间过长, 接着找到这个方法对应的是程序的哪一行代码, 
    你优化对应的代码就行了
(4) 将jobmanager或者taskmanager的stdout里面的日志下载下来, 用GCviewer工具来分析GC日志, 看看full GC之后
    新生代和老年代的空间大小

// 2. filnk数据倾斜
webui的subtasks中可以看到个别的subtask的接收数据量比其他的subtask明显大得多, 可能得相差几十倍, 这就说明出现了倾斜
一般有三种情形:

2.1 纯流式的, 也就是没有开窗, 开窗算是攒批了, 那么纯流式加上keyby操作出现倾斜, 这种情况你就不能纯用二次聚合解决了
    二次聚合你是先加随机数然后再去随机数, 这样两次聚合, reducer1-1 (1001_1,1) reducer-2 (1001, 1), 然后
    reducer1-2 (1001_1,1+1) reducer-2 (1001, 1+(1+1))最后结果得到了3, 结果错了, 因为你是来一条处理一条数据
    同时, 这种情况你第二次聚合数据量也没减少, 来几条还是处理几回

    这种情况你就得攒批了, 也就是攒批之后提前预聚合, local keyby

    (1) API的方式是自己实现, 思路很简单, flatmapFunction里面你来一条(a, 1)我用map存起来, 再来一条聚合一下(a, 2)
        直到来十条我才往下游发一回(a, 10), 同时你为了防止来不了10条, 你定一个时间, 时间一到就把数据往下发

    (2) 你用的sql的话, 开启参数就行了

2.2 那非纯流式的这种, 我既有keyBy也有开窗, 那么这种就是天生有攒批功能了, 那么你直接用二次聚合就行了
    map算子里面Tuple2.of(key + '_' + random.nextInt(randomNum), 1L), 接着该keyBy, window, reduce了都正常的操作,
    注意reduce的第二个参数窗口函数里面往下游发的时候用context加上窗口信息, 开始时间和结束时间, 假设得到的是
    Tuple3.of(key_random, count, windowEnd), 那之后你keyby的key是tuple2.of(key, windowEnd), 也就是原来的key加窗口结束时间分组再聚合

    sql写法会简单一点:

    select 
        winEnd,
        split_index(plat1,'_',0) as plat2,
        sum(pv) 
    from (
        select 
            TUMBLE_END(proc_time, INTERVAL '1' MINUTE) as winEnd,
            plat1,
            count(*) as pv 
        from (
            -- 最内层，将分组的key，也就是plat加上一个随机数打散
            select plat || '_' || cast(cast(RAND()*100 as int) as string) as plat1 ,proc_time 
            from source_kafka_table 
        ) group by TUMBLE(proc_time, INTERVAL '1' MINUTE), plat1
    ) group by winEnd,split_index(plat1,'_',0)

2.3 最最简单的一种, keyBy之前就存在数据倾斜, 这种很容易理解就是, 咱们程序中source算子的并行度是3, 然后kafka的
    分区数是3, 两者是1:1关系, 但是有种可能是本身kafka某分区的数据就比其他分区的数据多得多, 也就是不均匀, 这种
    会导致程序一开始就不行了, 那咱们可以使用重分区算子: shuffle、rebalance、rescale

2.4 什么场景出现的? 订单表和订单详情表要join, 会有倾斜