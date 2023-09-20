// 一、窗口迟到数据的处理
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env
                // a 1
                // a 2
                // ...
                .socketTextStream("localhost", 9999)
                // (a, 1000L)
                // (a, 2000L)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String in) throws Exception {
                        String[] array = in.split(" ");
                        return Tuple2.of(
                                array[0],
                                Long.parseLong(array[1]) * 1000L // 转换成毫秒单位
                        );
                    }
                })
                // 在map输出的数据流中插入水位线事件
                // 默认是200ms插入一次
                .assignTimestampsAndWatermarks(
                        // 将最大延迟时间设置为5秒钟
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1; // 将f1字段指定为事件时间字段
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 将迟到且对应窗口已经销毁的数据，发送到侧输出流中
                .sideOutputLateData(
                        // 泛型和窗口中的元素类型一致
                        new OutputTag<Tuple2<String, Long>>("late-event") {
                        }
                )
                // 等待迟到数据5秒钟
                .allowedLateness(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        out.collect("key：" + key + "，窗口：" + context.window().getStart() + "~" +
                                "" + context.window().getEnd() + "，里面有 " + elements.spliterator().getExactSizeIfKnown() + " 条数据。");
                    }
                });

        result.print("main");
        result.getSideOutput(new OutputTag<Tuple2<String, Long>>("late-event"){}).print("side");

        env.execute();
    }
}

// 二、实时热门商品
// 实时热门商品
// 统计滑动窗口(窗口长度1小时，滑动距离5分钟)里面浏览次数最多的3个商品
public class Example8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("/home/zuoyuan/flinktutorial0321/src/main/resources/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String in, Collector<UserBehavior> out) throws Exception {
                        String[] array = in.split(",");
                        UserBehavior userBehavior = new UserBehavior(
                                array[0], array[1], array[2], array[3],
                                Long.parseLong(array[4]) * 1000L
                        );
                        if (userBehavior.type.equals("pv")) {
                            out.collect(userBehavior);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.ts;
                            }
                        })
                )
                .keyBy(r -> r.productId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new CountAgg(), new WindowResult())
                .keyBy(new KeySelector<ProductViewCountPerWindow, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(ProductViewCountPerWindow in) throws Exception {
                        return Tuple2.of(in.windowStartTime, in.windowEndTime);
                    }
                })
                .process(new TopN(3))
                .print();

        env.execute();
    }

    public static class TopN extends KeyedProcessFunction<Tuple2<Long, Long>, ProductViewCountPerWindow, String> {
        private int n;

        public TopN(int n) {
            this.n = n;
        }

        private ListState<ProductViewCountPerWindow> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ProductViewCountPerWindow>(
                            "list-state",
                            Types.POJO(ProductViewCountPerWindow.class)
                    )
            );
        }

        @Override
        public void processElement(ProductViewCountPerWindow in, Context ctx, Collector<String> out) throws Exception {
            listState.add(in);

            // 保证所有的属于in.windowStartTime ~ in.windowEndTime的ProductViewCountPerWindow都添加到listState中
            ctx.timerService().registerEventTimeTimer(in.windowEndTime + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<ProductViewCountPerWindow> arrayList = new ArrayList<>();
            for (ProductViewCountPerWindow p : listState.get()) arrayList.add(p);
            // 手动gc
            listState.clear();

            arrayList.sort(new Comparator<ProductViewCountPerWindow>() {
                @Override
                public int compare(ProductViewCountPerWindow p1, ProductViewCountPerWindow p2) {
                    return (int)(p2.count - p1.count);
                }
            });

            StringBuilder result = new StringBuilder();
            result.append("============" + new Timestamp(ctx.getCurrentKey().f0) + "~" + new Timestamp(ctx.getCurrentKey().f1) + "============\n");
            for (int i = 0; i < n; i++) {
                ProductViewCountPerWindow tmp = arrayList.get(i);
                result.append("第" + (i+1) + "名的商品ID：" + tmp.productId + ",浏览次数：" + tmp.count + "\n");
            }
            result.append("===================================================================\n");
            out.collect(result.toString());
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long, ProductViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<ProductViewCountPerWindow> out) throws Exception {
            out.collect(new ProductViewCountPerWindow(
                    key,
                    elements.iterator().next(),
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }
}

// 三、窗口触发器trigger
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                // a 1
                // a 2
                // ...
                .socketTextStream("localhost", 9999)
                // (a, 1000L)
                // (a, 2000L)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String in) throws Exception {
                        String[] array = in.split(" ");
                        return Tuple2.of(
                                array[0],
                                Long.parseLong(array[1]) * 1000L // 转换成毫秒单位
                        );
                    }
                })
                // 在map输出的数据流中插入水位线事件
                // 默认是200ms插入一次
                .assignTimestampsAndWatermarks(
                        // 将最大延迟时间设置为5秒钟
                        // forBoundedOutOfOrderness(Duration.ofSeconds(0)) 等价于 forMonotonousTimestamps()
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1; // 将f1字段指定为事件时间字段
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(new Trigger<Tuple2<String, Long>, TimeWindow>() {
                    // 每来一条数据 触发一次调用
                    @Override
                    public TriggerResult onElement(Tuple2<String, Long> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.FIRE; // 触发窗口计算，也就是触发下游的process算子的执行
                    }

                    // 处理时间定时器：当机器时间到达`time`参数时，触发执行
                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    // 事件时间定时器：当水位线到达`time`参数时，触发执行
                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        if (time == window.getEnd() - 1L) {
                            return TriggerResult.FIRE_AND_PURGE; // 触发窗口计算并清空窗口
                        }
                        return TriggerResult.CONTINUE; // 什么都不做
                    }

                    // 当窗口闭合时触发执行
                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

                    }
                })
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        out.collect("key：" + key + "，窗口：" + context.window().getStart() + "~" +
                                "" + context.window().getEnd() + "，里面有 " + elements.spliterator().getExactSizeIfKnown() + " 条数据。");
                    }
                })
                .print();

        env.execute();
    }
}

// 四、使用hashSet计算UV
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("/home/zuoyuan/flinktutorial0321/src/main/resources/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String in, Collector<UserBehavior> out) throws Exception {
                        String[] array = in.split(",");
                        UserBehavior userBehavior = new UserBehavior(
                                array[0], array[1], array[2], array[3],
                                Long.parseLong(array[4]) * 1000L
                        );
                        if (userBehavior.type.equals("pv")) {
                            out.collect(userBehavior);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                )
                .keyBy(r -> "userbehavior")
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(
                        new AggregateFunction<UserBehavior, HashSet<String>, Long>() {
                            @Override
                            public HashSet<String> createAccumulator() {
                                return new HashSet<>();
                            }

                            @Override
                            public HashSet<String> add(UserBehavior value, HashSet<String> accumulator) {
                                accumulator.add(value.userId);
                                return accumulator;
                            }

                            @Override
                            public Long getResult(HashSet<String> accumulator) {
                                return (long) accumulator.size();
                            }

                            @Override
                            public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
                                return null;
                            }
                        },
                        new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
                                out.collect("窗口" + new Timestamp(context.window().getStart()) + "~" +
                                        "" + new Timestamp(context.window().getEnd()) + "的uv是：" +
                                        "" + elements.iterator().next());
                            }
                        }
                )
                .print();

        env.execute();

    }
}

// 五、使用布隆过滤器计算UV
public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("/home/zuoyuan/flinktutorial0321/src/main/resources/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String in, Collector<UserBehavior> out) throws Exception {
                        String[] array = in.split(",");
                        UserBehavior userBehavior = new UserBehavior(
                                array[0], array[1], array[2], array[3],
                                Long.parseLong(array[4]) * 1000L
                        );
                        if (userBehavior.type.equals("pv")) {
                            out.collect(userBehavior);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                )
                .keyBy(r -> "userbehavior")
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(
                        new AggregateFunction<UserBehavior, Tuple2<BloomFilter<String>, Long>, Long>() {
                            @Override
                            public Tuple2<BloomFilter<String>, Long> createAccumulator() {
                                return Tuple2.of(
                                        BloomFilter.create(
                                                // 去重的数据类型
                                                Funnels.stringFunnel(Charsets.UTF_8),
                                                // 预估的去重的数据量
                                                20000,
                                                // 误判率
                                                0.001
                                        ),
                                        0L // 计数器
                                );
                            }

                            @Override
                            public Tuple2<BloomFilter<String>, Long> add(UserBehavior value, Tuple2<BloomFilter<String>, Long> accumulator) {
                                // 如果用户之前一定没来过，计数器加一
                                if (!accumulator.f0.mightContain(value.userId)) {
                                    accumulator.f0.put(value.userId);
                                    accumulator.f1 += 1L;
                                }
                                return accumulator;
                            }

                            @Override
                            public Long getResult(Tuple2<BloomFilter<String>, Long> accumulator) {
                                return accumulator.f1;
                            }

                            @Override
                            public Tuple2<BloomFilter<String>, Long> merge(Tuple2<BloomFilter<String>, Long> a, Tuple2<BloomFilter<String>, Long> b) {
                                return null;
                            }
                        },
                        new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
                                out.collect("窗口" + new Timestamp(context.window().getStart()) + "~" +
                                        "" + new Timestamp(context.window().getEnd()) + "的uv是：" +
                                        "" + elements.iterator().next());
                            }
                        }
                )
                .print();

        env.execute();

    }
}

// 六、intervalJoin
//基于间隔的join
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> leftStream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("key-1", "left", 10 * 1000L), 10 * 1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        DataStreamSource<Event> rightStream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("key-1", "right", 2 * 1000L), 2 * 1000L);
                        ctx.collectWithTimestamp(new Event("key-1", "right", 8 * 1000L), 8 * 1000L);
                        ctx.collectWithTimestamp(new Event("key-1", "right", 12 * 1000L), 12 * 1000L);
                        ctx.collectWithTimestamp(new Event("key-1", "right", 22 * 1000L), 22 * 1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        leftStream.keyBy(r -> r.key)
                .intervalJoin(rightStream.keyBy(r -> r.key))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<Event, Event, String>() {
                    @Override
                    public void processElement(Event left, Event right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + " => " + right);
                    }
                })
                .print();

        env.execute();
    }
}

// 七、基于窗口的join
//基于窗口的join
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> leftStream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("key-1", "left", 2 * 1000L), 2 * 1000L);
                        ctx.collectWithTimestamp(new Event("key-1", "left", 6 * 1000L), 6 * 1000L);
                        ctx.collectWithTimestamp(new Event("key-2", "left", 13 * 1000L), 13 * 1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        DataStreamSource<Event> rightStream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("key-1", "right", 2 * 1000L), 2 * 1000L);
                        ctx.collectWithTimestamp(new Event("key-1", "right", 8 * 1000L), 8 * 1000L);
                        ctx.collectWithTimestamp(new Event("key-2", "right", 12 * 1000L), 12 * 1000L);
                        ctx.collectWithTimestamp(new Event("key-2", "right", 22 * 1000L), 22 * 1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        leftStream
                .join(rightStream)
                .where(r -> r.key)
                .equalTo(r -> r.key)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Event, Event, String>() {
                    @Override
                    public String join(Event first, Event second) throws Exception {
                        return first + " => " + second;
                    }
                })
                .print();

        env.execute();
    }
}

// 八、使用flink-cep检测连续三次登录失败
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("user-1", "fail", 1000L), 1000L);
                        ctx.collectWithTimestamp(new Event("user-1", "fail", 2000L), 2000L);
                        ctx.collectWithTimestamp(new Event("user-2", "success", 3000L), 3000L);
                        ctx.collectWithTimestamp(new Event("user-1", "fail", 4000L), 4000L);
                        ctx.collectWithTimestamp(new Event("user-1", "fail", 5000L), 5000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        // 定义模板
        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("first")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event in) throws Exception {
                        return in.value.equals("fail");
                    }
                })
                // 表示紧邻first事件
                .next("second")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event in) throws Exception {
                        return in.value.equals("fail");
                    }
                })
                .next("third")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event in) throws Exception {
                        return in.value.equals("fail");
                    }
                });

        // 使用模板在数据流上检测连续三次登录失败
        CEP
                .pattern(stream.keyBy(r -> r.key), pattern)
                .select(new PatternSelectFunction<Event, String>() {
                    @Override
                    public String select(Map<String, List<Event>> map) throws Exception {
                        // {
                        //   "first": [Event],
                        //   "second": [Event],
                        //   "third": [Event]
                        // }
                        Event first = map.get("first").get(0);
                        Event second = map.get("second").get(0);
                        Event third = map.get("third").get(0);

                        return first.key + "连续3次登录失败，登录时间戳是：" + first.ts + ";" + second.ts + ";" + third.ts;
                    }
                })
                .print();


        env.execute();
    }
}

// 九、对三次登录失败的优化
// 使用flink-cep检测连续三次登录失败
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("user-1", "fail", 1000L), 1000L);
                        ctx.collectWithTimestamp(new Event("user-1", "fail", 2000L), 2000L);
                        ctx.collectWithTimestamp(new Event("user-2", "success", 3000L), 3000L);
                        ctx.collectWithTimestamp(new Event("user-1", "fail", 4000L), 4000L);
                        ctx.collectWithTimestamp(new Event("user-1", "fail", 5000L), 5000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        // 定义模板
        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("login-fail")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event in) throws Exception {
                        return in.value.equals("fail");
                    }
                })
                // 发生3次
                .times(3)
                // 要求3次事件紧邻发生
                .consecutive();


        // 使用模板在数据流上检测连续三次登录失败
        CEP
                .pattern(stream.keyBy(r -> r.key), pattern)
                .select(new PatternSelectFunction<Event, String>() {
                    @Override
                    public String select(Map<String, List<Event>> map) throws Exception {
                        // {
                        //   "login-fail": [Event, Event, Event],
                        // }
                        Event first = map.get("login-fail").get(0);
                        Event second = map.get("login-fail").get(1);
                        Event third = map.get("login-fail").get(2);

                        return first.key + "连续3次登录失败，登录时间戳是：" + first.ts + ";" + second.ts + ";" + third.ts;
                    }
                })
                .print();


        env.execute();
    }
}

// 十、 使用flink-cep实现订单超时检测
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<Event, String> stream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("order-1", "create-order", 1000L), 1000L);
                        ctx.collectWithTimestamp(new Event("order-2", "create-order", 2000L), 2000L);
                        ctx.collectWithTimestamp(new Event("order-1", "pay-order", 3000L), 3000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> r.key);

        // 定义模板
        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("create")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event in) throws Exception {
                        return in.value.equals("create-order");
                    }
                })
                .next("pay")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event in) throws Exception {
                        return in.value.equals("pay-order");
                    }
                })
                // 要求两个事件在5秒钟之内发生
                .within(Time.seconds(5));

        SingleOutputStreamOperator<String> result = CEP
                .pattern(stream, pattern)
                .flatSelect(
                        // 侧输出标签，用来接收超时信息
                        new OutputTag<String>("timeout-info") {
                        },
                        // 匿名类，用来发送超时信息
                        new PatternFlatTimeoutFunction<Event, String>() {
                            @Override
                            public void timeout(Map<String, List<Event>> map, long l, Collector<String> collector) throws Exception {
                                // map {
                                //  "create": [Event]
                                // }
                                Event create = map.get("create").get(0);
                                // 发送信息到侧输出流
                                collector.collect(create.key + "超时未支付");
                            }
                        },
                        // 匿名类，用来发送已支付的信息
                        new PatternFlatSelectFunction<Event, String>() {
                            @Override
                            public void flatSelect(Map<String, List<Event>> map, Collector<String> collector) throws Exception {
                                // map {
                                //  "create": [Event],
                                //  "pay": [Event]
                                // }
                                Event create = map.get("create").get(0);
                                Event pay = map.get("pay").get(0);
                                collector.collect(create.key + "在" + pay.ts + "完成支付");
                            }
                        }
                );


        result.print("main");

        result.getSideOutput(new OutputTag<String>("timeout-info"){}).print("side");

        env.execute();
    }
}

// 十一、使用状态实现订单超时检测
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<Event, String> stream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("order-1", "create-order", 1000L), 1000L);
                        ctx.collectWithTimestamp(new Event("order-2", "create-order", 2000L), 2000L);
                        ctx.collectWithTimestamp(new Event("order-1", "pay-order", 3000L), 3000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> r.key);

        stream
                .process(new KeyedProcessFunction<String, Event, String>() {
                    private ValueState<Event> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(
                                new ValueStateDescriptor<Event>(
                                        "state",
                                        Types.POJO(Event.class)
                                )
                        );
                    }

                    @Override
                    public void processElement(Event in, Context ctx, Collector<String> out) throws Exception {
                        if (in.value.equals("create-order")) {
                            state.update(in);
                            ctx.timerService().registerEventTimeTimer(in.ts + 5000L);
                        } else if (in.value.equals("pay-order")) {
                            out.collect(in.key + "在" + in.ts + "完成支付");
                            state.clear();
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        if (state.value() != null) {
                            out.collect(state.value().key + "超时未支付");
                        }
                    }
                })
                .print();

        env.execute();
    }
}