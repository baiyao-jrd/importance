// 1. flink多流合并
1.1 union -> 可以连接n条流, 但n条流的数据类型必须得一致

DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3);
DataStreamSource<Integer> ds2 = env.fromElements(2, 2, 3);
DataStreamSource<String> ds3 = env.fromElements("2", "2", "3");

ds1
    .union(
        ds2,
        ds3.map(Integer::valueOf)
    )
    .print();

1.2 connect -> 一次只能连接 2条流; 流的数据类型可以不一样; 连接后可以调用 map、flatmap、process来处理, 但是各处理各的

1.2.1 .map() -> 传入一个CoMapFunction, 需要实现map1()、map2()两个方法

DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3);
DataStreamSource<String> source2 = env.fromElements("a", "b", "c");

ConnectedStreams<Integer, String> connect = source1.connect(source2);

connect
    .map(
        new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "来源于数字流:" + value.toString();
            }

            @Override
            public String map2(String value) throws Exception {
                return "来源于字母流:" + value;
            }   
        }
    );

1.2.2 .process() -> 传入一个CoProcessFunction, 需要实现processElement1()、processElement2()两个方法
// 实现inner join的效果
source1 = env.fromElements(
    Tuple2.of(1, "a1"),
    Tuple2.of(1, "a2"),
    Tuple2.of(2, "b"),
    Tuple2.of(3, "c")
);

source2 = env.fromElements(
    Tuple3.of(1, "aa1", 1),
    Tuple3.of(1, "aa2", 2),
    Tuple3.of(2, "bb", 1),
    Tuple3.of(3, "cc", 1)
);

ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connect = source1.connect(source2);

// 多并行度下, 需要根据 关联条件 进行keyby, 才能保证key相同的数据到一起去, 才能匹配上
connect
    .keyBy(s1 -> s1.f0, s2 -> s2.f0)
    .process(
        new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {

            // 定义 HashMap, 缓存来过的数据, key=id, value=list<数据>
            Map<Integer, List<Tuple2<Integer, String>>> s1Cache = new HashMap<>();
            Map<Integer, List<Tuple3<Integer, String, Integer>>> s2Cache = new HashMap<>();

            @Override
            public void processElement1(Tuple2<Integer, String> value, Context ctx, Collector<String> out) throws Exception {
                Integer id = value.f0;
                // TODO 1.来过的s1数据, 都存起来
                if (!s1Cache.containsKey(id)) {
                    // 1.1 第一条数据, 初始化 value的list, 放入 hashmap
                    List<Tuple2<Integer, String>> s1Values = new ArrayList<>();
                    s1Values.add(value);
                    s1Cache.put(id, s1Values);
                } else {
                    // 1.2 不是第一条, 直接添加到 list中
                    s1Cache.get(id).add(value);
                }

                //TODO 2.根据id, 查找s2的数据, 只输出 匹配上 的数据
                if (s2Cache.containsKey(id)) {
                    for (Tuple3<Integer, String, Integer> s2Element : s2Cache.get(id)) {
                        out.collect("s1:" + value + "<--------->s2:" + s2Element);
                    }
                }
            }

            @Override
            public void processElement2(Tuple3<Integer, String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                Integer id = value.f0;
                // TODO 1.来过的s2数据, 都存起来
                if (!s2Cache.containsKey(id)) {
                    // 1.1 第一条数据, 初始化 value的list, 放入 hashmap
                    List<Tuple3<Integer, String, Integer>> s2Values = new ArrayList<>();
                    s2Values.add(value);
                    s2Cache.put(id, s2Values);
                } else {
                    // 1.2 不是第一条, 直接添加到 list中
                    s2Cache.get(id).add(value);
                }

                //TODO 2.根据id, 查找s1的数据, 只输出 匹配上 的数据
                if (s1Cache.containsKey(id)) {
                    for (Tuple2<Integer, String> s1Element : s1Cache.get(id)) {
                        out.collect("s1:" + s1Element + "<--------->s2:" + value);
                    }
                }
            }
        });

result.print(); 

1.3 window Join

   /**
    * 类似于inner join
    * 最后输出的只有两条流中按key配对成功的数据
    * 如果某个窗口中一条流的数据没有任何另一条流的数据匹配, 那么就不会调用JoinFunction的join()方法, 也就没有任何输出了

    * 效果类似于: SELECT * FROM table1 t1, table2 t2 WHERE t1.id = t2.id; 

    */

stream1
    .join(stream2)
    // 指定第一条流的key
    .where(<KeySelector>)
    // 指定第二条流的key
    .equalTo(<KeySelector>)
    // 滚动, 滑动, 会话 均可
    .window(<WindowAssigner>)
    // 这里只能调.apply(), 传入JoinFunction接口, 实现.join()方法
    .apply(<JoinFunction>)

// ****************************************************************

ds1 = env
    .fromElements(
        Tuple2.of("a", 1),
        Tuple2.of("a", 2),
        Tuple2.of("b", 3),
        Tuple2.of("c", 4)
    )
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<Tuple2<String, Integer>>forMonotonousTimestamps()
            .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
    );


ds2 = env
    .fromElements(
        Tuple3.of("a", 1,1),
        Tuple3.of("a", 11,1),
        Tuple3.of("b", 2,1),
        Tuple3.of("b", 12,1),
        Tuple3.of("c", 14,1),
        Tuple3.of("d", 15,1)
    )
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<Tuple3<String, Integer,Integer>>forMonotonousTimestamps()
            .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
    );

// TODO window join
// 1. 落在同一个时间窗口范围内才能匹配
// 2. 根据keyby的key, 来进行匹配关联
// 3. 只能拿到匹配上的数据, 类似有固定时间范围的inner join

ds1
    .join(ds2)
    .where(r1 -> r1.f0)  // ds1的keyby
    .equalTo(r2 -> r2.f0) // ds2的keyby
    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
    .apply(
        new JoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
            /**
                * 关联上的数据, 调用join方法
                * @param first  ds1的数据
                * @param second ds2的数据
                */
            @Override
            public String join(Tuple2<String, Integer> first, Tuple3<String, Integer, Integer> second) throws Exception {
                return first + "<----->" + second;
            }
        }
    );

1.4 interval Join -> 间隔联结的两条流A和B, 必须基于相同的key, 且只支持事件时间语义

   /**
    * 针对一条流的每个数据, 开辟出其时间戳前后的一段闭区间时间间隔 [a.timestamp + lowerBound, a.timestamp + upperBound]
    * 看这期间是否有来自另一条流的数据匹配。这段时间作为可以匹配另一条流数据的 "窗口" 范围。

    * interval Join同样是内连接(inner join), 但是跟window Join相比, interval join的"窗口"范围基于流中数据, 所以不确定
    * 而且流B中的数据可以不只在一个区间内被匹配。

    */

stream1
    .keyBy(<KeySelector>)
    .intervalJoin(stream2.keyBy(<KeySelector>))
    .between(Time.milliseconds(-2), Time.milliseconds(1))
    .process(
        new ProcessJoinFunction<Integer, Integer, String(){
            @Override
            public void processElement(Integer left, Integer right, Context ctx, Collector<String> out) {
                out.collect(left + "," + right);
            }
        }
    );

1.4.1 正常使用案例

有两条流, 一条是下单流, 一条是浏览数据的流。针对同一用户, 来做联结。也就是使用一个用户的下单事件和这个用户的最近十分钟的浏览数据进行一个联结查询。

ds1 = env
    .fromElements(
        Tuple2.of("a", 1),
        Tuple2.of("a", 2),
        Tuple2.of("b", 3),
        Tuple2.of("c", 4)
    )
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<Tuple2<String, Integer>>forMonotonousTimestamps()
            .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
    );


ds2 = env
    .fromElements(
        Tuple3.of("a", 1, 1),
        Tuple3.of("a", 11, 1),
        Tuple3.of("b", 2, 1),
        Tuple3.of("b", 12, 1),
        Tuple3.of("c", 14, 1),
        Tuple3.of("d", 15, 1)
    )
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
            .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
    );

    // TODO interval join
    //1. 分别做keyby, key其实就是关联条件
    ds1.keyBy(r1 -> r1.f0);
    ds2.keyBy(r2 -> r2.f0);

    //2. 调用 interval join
    ks1.intervalJoin(ks2)
        .between(Time.seconds(-2), Time.seconds(2))
        .process(
            new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                /**
                    * 两条流的数据匹配上, 才会调用这个方法
                    * @param left  ks1的数据
                    * @param right ks2的数据
                    * @param ctx   上下文
                    * @param out   采集器
                    */
                @Override
                public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, Context ctx, Collector<String> out) throws Exception {
                    // 进入这个方法, 是关联上的数据
                    out.collect(left + "<------>" + right);
                }
            }
        )
        .print();

1.4.2 处理迟到数据

ds1 = env
    .socketTextStream("hadoop102", 7777)
    .map(new MapFunction<String, Tuple2<String, Integer>>() {
        @Override
        public Tuple2<String, Integer> map(String value) throws Exception {
            String[] datas = value.split(",");
            return Tuple2.of(datas[0], Integer.valueOf(datas[1]));
        }
    })
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
            .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
    );


ds2 = env
    .socketTextStream("hadoop102", 8888)
    .map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
        @Override
        public Tuple3<String, Integer, Integer> map(String value) throws Exception {
            String[] datas = value.split(",");
            return Tuple3.of(datas[0], Integer.valueOf(datas[1]), Integer.valueOf(datas[2]));
        }
    })
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
            .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
    );

/**
    * TODO Interval join
    * 1、只支持事件时间
    * 2、指定上界、下界的偏移, 负号代表时间往前, 正号代表时间往后
    * 3、process中, 只能处理 join上的数据
    * 4、两条流关联后的watermark, 以两条流中最小的为准
    * 5、如果 当前数据的事件时间 < 当前的watermark, 就是迟到数据,  主流的process不处理
    *  => between后, 可以指定将 左流 或 右流 的迟到数据 放入侧输出流
    */

//1. 分别做keyby, key其实就是关联条件
ks1 = ds1.keyBy(r1 -> r1.f0);
ks2 = ds2.keyBy(r2 -> r2.f0);

//2. 调用 interval join
ks1LateTag = new OutputTag<>("ks1-late", Types.TUPLE(Types.STRING, Types.INT));
ks2LateTag = new OutputTag<>("ks2-late", Types.TUPLE(Types.STRING, Types.INT, Types.INT));

ks1
    .intervalJoin(ks2)
    .between(Time.seconds(-2), Time.seconds(2))
    .sideOutputLeftLateData(ks1LateTag)  // 将 ks1的迟到数据, 放入侧输出流
    .sideOutputRightLateData(ks2LateTag) // 将 ks2的迟到数据, 放入侧输出流
    .process(
        new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
            /**
                * 两条流的数据匹配上, 才会调用这个方法
                * @param left  ks1的数据
                * @param right ks2的数据
                * @param ctx   上下文
                * @param out   采集器
                */
            @Override
            public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, Context ctx, Collector<String> out) throws Exception {
                // 进入这个方法, 是关联上的数据
                out.collect(left + "<------>" + right);
            }
        }
    );

process.print("主流");
process.getSideOutput(ks1LateTag).printToErr("ks1迟到数据");
process.getSideOutput(ks2LateTag).printToErr("ks2迟到数据");