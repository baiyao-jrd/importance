// 1. flinkSQL优化
1.1 miniBatch -> 攒批
    攒批的目的是提高程序的吞吐量, 你一次处理1条和一次处理100条效率是不一样的, 你吃饭一次吃一粒米肯定没你
    一次吃一大口米的效率高, 那么你攒批攒多长时间呢, 一般秒级分钟级都行, 因为一般企业的程序处理都是分钟级别的
    毫秒级别的处理速度对资源要求很高, 没几家公司能做到

1.2 localGlobal -> 提前combiner
    就是先本地把相同key的预先聚合起来, 然后再全局性的把各个地方相同key的给聚合起来
    localGlobal的前提是开启minnibatch, 你不攒批就不会有本地聚合

注意, 1.1的miniBatch -> 攒批, 加上1.2 localGlobal -> 提前combiner, 就可以解决数据倾斜的场景了

1.3 split-distinct -> 这是你用了count(distinct)的场景
    count(distinct)做了一件事儿就是全局去重统计, 那么就会出现所有的key出现在了一个reducer中, 如果你单独依赖
    miniBatch和localGlobal的话, 效率没有那么高, 所以又设计出了split-distinct

    开启参数：

    // 获取 tableEnv的配置对象
    Configuration configuration = tEnv.getConfig().getConfiguration();

    // 设置参数：(要结合minibatch一起使用)
    // 开启Split Distinct
    configuration.setString("table.optimizer.distinct-agg.split.enabled", "true");
    // 第一层打散的bucket数目
    configuration.setString("table.optimizer.distinct-agg.split.bucket-num", "1024");

    相当于是把不同key哈希到不同桶里面了, 那么相同key的数据就在一个桶里面, 这个通就可以按key去重

1.4 使用filter语法 -> 特定场景, 就是你要多次使用count(distinct a), 对同一个字段多次distinct
    你针对不同条件统计uv, 比如要统计全局uv, web端uv, app端uv

    SELECT
        a,
        COUNT(DISTINCT b) AS total_b,
        COUNT(DISTINCT CASE WHEN c IN ('A', 'B') THEN b ELSE NULL END) AS AB_b,
        COUNT(DISTINCT CASE WHEN c IN ('C', 'D') THEN b ELSE NULL END) AS CD_b
    FROM T
    GROUP BY a

    这里distinct的都是b, 但是会维护3个状态, 但是你写成下面的形式就会只维护一个状态

    SELECT
        a,
        COUNT(DISTINCT b) AS total_b,
        COUNT(DISTINCT b) FILTER (WHERE c IN ('A', 'B')) AS AB_b,
        COUNT(DISTINCT b) FILTER (WHERE c IN ('C', 'D')) AS CD_b
    FROM T
    GROUP BY a

1.5 所有设置参数总结
// 初始化table environment
TableEnvironment tEnv = ...

// 获取 tableEnv的配置对象
Configuration configuration = tEnv.getConfig().getConfiguration();

// 设置参数：


// 开启miniBatch
configuration.setString("table.exec.mini-batch.enabled", "true");
// 批量输出的间隔时间
configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
// 防止OOM设置每个批次最多缓存数据的条数，可以设为2万条
configuration.setString("table.exec.mini-batch.size", "20000");
// 开启LocalGlobal
configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
// 开启Split Distinct
configuration.setString("table.optimizer.distinct-agg.split.enabled", "true");
// 第一层打散的bucket数目
configuration.setString("table.optimizer.distinct-agg.split.bucket-num", "1024");
// 指定时区
configuration.setString("table.local-time-zone", "Asia/Shanghai");