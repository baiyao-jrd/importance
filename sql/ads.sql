-- ADS层指标(9个)

-- /*********************************************** 1. 流量主题 - 各渠道流量统计 *****************************************************/

-- ads_traffic_stats_by_channel
-- dws层 -> 因为查的时候全查, 不会固定拿某些列, 所以不需要列式存储, 因为数据量太小所以也不需要压缩
--       -> 由于每天的聚合数据量几十条, 分区的话会产生小文件, 所以也没有分区 


-- 统计周期	        统计粒度	    指标	                说明
-- 最近1/7/30日	    渠道	        访客数	                统计访问人数
-- 最近1/7/30日	    渠道	        会话平均停留时长	     统计每个会话平均停留时长
-- 最近1/7/30日	    渠道	        会话平均浏览页面数	     统计每个会话平均浏览页面数
-- 最近1/7/30日	    渠道	        会话总数	            统计会话总数
-- 最近1/7/30日	    渠道	        跳出率	                只有一个页面的会话的比例


-- 含义: 假设渠道有5个, 现在要统计1,7,30天的各渠道指标, 那么最后就会得到15行数据

insert overwrite table ads_traffic_stats_by_channel
-- 由于表没有分区, 你每次insert overwrite覆盖的都是全表, 所以你就需要union历史数据后再overwrite
-- 不能直接用insert into, 因为你当天最新的数据装载sql重复执行一遍就不幂等了, 另外每次insert into的时候, 他都会产生新文件,
-- 这样每天的数据量很小就会产生大量小文件

select * from ads_traffic_stats_by_channel

-- 这里的union可以在你这个sql重复执行的情况下, 去重, 用重复执行的结果覆盖了原来的数据
union
select
    '2020-06-14' as dt,    -- '统计日期'
    -- 不能只写一个dt, 下面除了聚合函数, 结果一定是一个值, 你这里的dt如果不直接指定, 他就代表多个不确定的值

    recent_days,        -- '最近天数,1:最近1天,7:最近7天,30:最近30天' 
    -- -> 代表统计周期

    channel,            -- '渠道' 
    -- -> 代表统计粒度

    count(distinct(mid_id)) uv_count,           -- '访客人数'
    -- 使用mid标识一个人

    avg(during_time_1d)/1000 avg_duration_sec,  -- '会话平均停留时长，单位为秒'
    -- 会话1(pageA 3s, pageB 4s, pageC 5s) -> 总时长: 3+4+5=12
    -- 会话2(pageA 6s, pageB 7s, pageC 8s) -> 总时长: 6+7+8=21
    -- 会话3(pageA 9s, pageB 10s, pageC 11s) -> 总时长: 9+10+11=30
    -- 平均时长: (12+21+30) / 3

    -- 由于咱们拿的是会话粒度的汇总表, 每行就代表了一个会话的总during_time, 所以想求会话平均停留时长, 直接求平均就行了

    avg(page_count_1d) avg_page_count,          -- '会话平均浏览页面数'
    -- 会话1(pageA, pageB, pageC) -> 总浏览数: 3
    -- 会话2(pageA) -> 总浏览数: 1
    -- 会话3(pageA, pageB) -> 总浏览数: 2
    -- 平均浏览页面数: (3+1+2) / 3

    -- page_count_1d -> 代表这个会话一共浏览了多少个页面

    count(*) sv_count,                          -- '会话数'

    sum(if(page_count_1d=1,1,0))/count(*) bounce_rate   -- '跳出率'
    -- 跳出会话的个数 / 总会话数 
    -- 跳出会话 -> 这个会话里面只浏览了一个页面
    -- 跳出率 -> 只有一个页面的会话 占 所有会话数 的 比例

from dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days
-- UDTF炸裂函数
-- dws_traffic_session_page_view_1d -> 流量域 最近一天 各会话的页面浏览数
-- explode(array(1,7,30)) -> 炸裂之后, 原表数据量会放大为3倍

where dt >= date_add('2020-06-14', -recent_days+1)
-- 从炸裂的3份儿表中筛选出对应分区的数据
-- 最近一天只选当天的, 最近7天就选最近七天的, 最近30天就选最近30天的

group by recent_days,channel;
-- 按照统计周期, 统计粒度进行分组

-- /*********************************************** 2. 流量主题 - 用户访问路径分析 *****************************************************/

-- 了解用户行为偏好, 要对访问路径进行分析。需要提供每种页面跳转的次数, 每个跳转由source/target表示, source指跳转起始页面, target表示跳转终到页面
-- 桑基图要求 1. source不能为空 2. 不能存在环儿(A -> B -> A)

insert overwrite table ads_page_path
select * from ads_page_path
union
select
    '2020-06-14' dt,        -- '统计日期'
    recent_days,            -- '最近天数,1:最近1天,7:最近7天,30:最近30天'
    source,                 -- '跳转起始页面ID'
    nvl(target,'null'),     -- '跳转终到页面ID'
    -- concat函数在连接时,如果其中任何一个参数为null,则结果将是null。
    -- 这里要对为null的转换为字符串null进行存储

    count(*) path_count     -- '跳转次数'
from
(
    select
        recent_days,
        concat('step-',rn,':',page_id) source,
        concat('step-',rn+1,':',next_page_id) target
    from
    (
        select
            recent_days,
            page_id,
            -- 使用本页id作为source, 就避免了source为空的情况

            lead(page_id,1,null) over(partition by session_id,recent_days) next_page_id,
            -- 下页id, 作为target

            row_number() over (partition by session_id,recent_days order by view_time) rn
            -- 给访问页面名添加序号, 就避免了桑基图形成环
            -- 本次统计周期和本次会话内的递增序号

        from dwd_traffic_page_view_inc lateral view explode(array(1,7,30)) tmp as recent_days
        -- dwd_traffic_page_view_inc -> 某一天某个会话, 访问的上个页面是啥, 本页面是啥, 访问的时间点
        where dt>=date_add('2020-06-14',-recent_days+1)
    )t1
)t2
group by recent_days,source,target;

-- /*********************************************** 3. 用户主题 - 用户变动统计 *****************************************************/

-- 用户变动: 
-- 1. 流失用户数 -> 
-- 2. 回流用户数 -> 
-- 统计周期	    指标	        说明
-- 最近1日	    流失用户数	    之前活跃过，最近一段时间未活跃。
--                            要求统计7日前(只包含7日前当天)活跃, 但最近7日未活跃的用户总数。
--                            这里统计的是每天新增的流失用户数。

-- 最近1日	    回流用户数	    之前的活跃用户，一段时间未活跃（流失），今日又活跃了，就称为回流用户。
--                            要求统计回流用户总数。

-- 注意什么叫当天新增的流失用户, 什么叫当天的流失用户总数
-- A用户
-- _活跃_, _未活跃第1天_, _未活跃第2天_,
-- _未活跃第3天_, _未活跃第4天_, _未活跃第5天_,
-- _未活跃第6天_, _未活跃第7天_                     -> 注意未活跃的第7天, A用户就算做这一天的新增流失用户
-- _未活跃第8天_                                   -> 注意第8天依然未活跃, 但是A用户仍属于流失用户, 但是不属于第八天的新增流失用户

-- 当天的回流用户
-- B用户
-- _活跃_,
-- _未活跃第1天_, _未活跃第2天_, _未活跃第3天_, _未活跃第4天_,
-- _未活跃第5天_, _未活跃第6天_, _未活跃第7天_,
-- _活跃_,                                         -> 第8天他活跃了, 就认定B用户是今天的回流用户, 注意中间流失的天数可以 >= 7

-- 活跃 -- login 即为活跃
-- 业务过程对应的是登录

-- 每天就一行数据
insert overwrite table ads_user_change
select * from ads_user_change
union
select
    churn.dt,
    user_churn_count,
    user_back_count
from
(
    select
        '2020-06-14' dt,
        count(*) user_churn_count
    from dws_user_user_login_td

    -- dws_user_user_login_td -> 用户域 用户粒度 登录历史至今 汇总事实表
    -- login_date_last 末次活跃日期
    -- user_id  login_date_last     login_count_td      dt
    -- 2        2020-06-10          1                   2020-06-14

    -- td表是一天一个分区, 每天分区存放的就是历史至今全部登录记录的汇总结果

    where dt='2020-06-14'
    and login_date_last = date_add('2020-06-14',-7)
    -- 06-14恰好是未活跃的第七天, 而上次活跃就是06-07, 所以差7
    
) churn join (

    select
        '2020-06-14' dt,
        count(*) user_back_count
    from
    (
        -- 今天的回流用户一定是今天的活跃用户
        -- 今天的所有回流用户的末次活跃日期一定是今天
        -- 它的上次登录日期会与今天的日期相隔7天及以上
        select
            user_id,
            login_date_last
        from dws_user_user_login_td
        -- 每天这张表都会算一下所有用户的最后一次活跃日期

        where dt='2020-06-14'
    )t1 join (
        select
            user_id,
            login_date_last login_date_previous
        from dws_user_user_login_td
        where dt=date_add('2020-06-14',-1)
        -- 拿活跃日期的前一天的数据, 就能拿到末次活跃的上次活跃日期
    )t2
    on t1.user_id=t2.user_id
    where datediff(login_date_last,login_date_previous)>=8
) back
on churn.dt=back.dt;


-- /*********************************************** 4. 用户主题 - 用户留存率统计 *****************************************************/

-- 留存分析: 
-- 1. 新增留存 -> 某天的新增用户中，有多少人有后续的活跃行为。
-- 2. 活跃留存 -> 某天的活跃用户中，有多少人有后续的活跃行为。

-- 此处要求统计新增留存率: 
-- 留存用户数与新增用户数的比值
-- 例如2020-06-14新增100个用户, 1日之后这100人中有80个人活跃了, 那2020-06-14的1日留存率则为80%。

-- 谈留存数或者留存率的时候一定要有定语 -> 哪一天的几日
-- (2020-06-14)的(1日)留存率

-- 这里要统计每天的1~7日留存率
-- (2020-06-14)的(1日)留存率
-- 需要拿到14号的新增以及15号的活跃数据

-- (2020-06-14)的(2日)留存率
-- (2020-06-14)的(3日)留存率
-- (2020-06-14)的(4日)留存率
-- (2020-06-14)的(5日)留存率
-- (2020-06-14)的(6日)留存率
-- (2020-06-14)的(7日)留存率

-- 这一行数据就代表这是哪一天的几日留存
-- 这一天是几号, 新增用户是多少, n天后留存用户多少, 作除
-- 每一天会往表里面写7条数据

insert overwrite table ads_user_retention
select * from ads_user_retention
union
select
    '2020-06-14' dt,                -- '统计日期'
    login_date_first create_date,   -- '用户新增日期'           -> 统计了这一天用户新增了多少
    datediff('2020-06-14',login_date_first) retention_day,      -- '截至当前日期留存天数'
    -- 分组字段这个呢没有它, 但是有login_date_first, 所以这里基于分组字段的udf函数也是可以选的 -> 1进1出 

    sum(if(login_date_last='2020-06-14',1,0)) retention_count,  -- '留存用户数量'、
    -- 通过14号的数据拿到每个新增用户的上次登录日期, 如果是14号当天, 就是当天的留存用户

    count(*) new_user_count,                                    -- '新增用户数量'
    sum(if(login_date_last='2020-06-14',1,0))/count(*)*100 retention_rate    -- '留存率'
from (
    select
        user_id,
        date_id login_date_first
    from dwd_user_register_inc
    -- 一天一个分区, 每个分区只存放当天的注册记录
    where dt>=date_add('2020-06-14',-7)
    and dt<'2020-06-14'
    -- 拿到7~13号的新增用户
)t1 join (
    -- 两者join就拿到了, 某个人是login_date_first天新增的, 另外他的上次活跃日期也得拿到
    select
        user_id,
        login_date_last
    from dws_user_user_login_td
    where dt='2020-06-14'
)t2
on t1.user_id=t2.user_id
group by login_date_first;

-- /*********************************************** 5. 用户主题 - 用户新增/活跃统计 *****************************************************/

-- 统计周期	        指标	        指标说明
-- 最近1、7、30日	新增用户数	     略
-- 最近1、7、30日	活跃用户数	     略

-- 每天会插3条数据
insert overwrite table ads_user_stats
select * from ads_user_stats
union
select
    '2020-06-14' dt,        -- '统计日期'
    t1.recent_days,         -- '最近n日,1:最近1日,7:最近7日,30:最近30日'
    new_user_count,         -- '新增用户数'
    active_user_count       -- '活跃用户数'
from (
    -- 新增用户: 最近n天注册了多少用户
    select
        -- 活跃用户: 最近n天登录了多少用户
        -- 要注意去重, 假设一个用户在最近7天, 每天都登陆了, 这个用户只能算一次, 而这里的正好是用户粒度的历史至今表
        recent_days,
        sum(if(login_date_last>=date_add('2020-06-14',-recent_days+1),1,0)) new_user_count
    from dws_user_user_login_td lateral view explode(array(1,7,30)) tmp as recent_days
    -- 所有用户历史至今的末次登陆日期, 每个用户一个日期
    where dt='2020-06-14'
    group by recent_days
)t1 join (
    -- 最近1,7,30的新增用户数
    select
        recent_days,
        sum(if(date_id>=date_add('2020-06-14',-recent_days+1),1,0)) active_user_count
    from dwd_user_register_inc lateral view explode(array(1,7,30)) tmp as recent_days
    group by recent_days
)t2
on t1.recent_days=t2.recent_days;

-- /*********************************************** 6. 用户主题 - 用户行为漏斗分析 *****************************************************/

-- 漏斗分析能反映一个业务流程从起点到终点各阶段用户转化情况。那么哪个阶段出现问题就能一目了然。

-- 这个需求就是统计一个完整的购物流程中各个阶段的人数
-- 浏览首页人数 -> 浏览详情页人数 -> 加购物车人数 -> 下单人数 -> 支付人数
-- mid            mid              user_id        user_id    user_id

-- 每天会往这个表里面写三行数据
insert overwrite table ads_user_action
select * from ads_user_action
union
select
    '2020-06-14' dt,            -- '统计日期'
    page.recent_days,           -- '最近天数,1:最近1天,7:最近7天,30:最近30天'
    home_count,                 -- '浏览首页人数'
    good_detail_count,          -- '浏览商品详情页人数'
    cart_count,                 -- '加入购物车人数'
    order_count,                -- '下单人数'
    payment_count               -- '支付人数'
from
(
    select
        1 recent_days,
        sum(if(page_id='home',1,0)) home_count,
        sum(if(page_id='good_detail',1,0)) good_detail_count
    from dws_traffic_page_visitor_page_view_1d
    -- 流量域访客页面粒度页面浏览最近1日汇总事实表
    -- 某一个天, 某个mid访问同一个页面的聚合记录

    where dt='2020-06-14'
    and page_id in ('home','good_detail')
    union all
    select
        recent_days,
        -- 由于nd表咱们是拿的最近30天的数据进行聚合, 有可能你最近7天没有访问过这个home或者good_detail的页面
        -- 但是你最近30天访问过了, 怎么区别这个, 使用view_count标识, 大于零再加加操作
        sum(if(page_id='home' and view_count>0,1,0)),
        sum(if(page_id='good_detail' and view_count>0,1,0))
    from
    (
        select
            recent_days,
            page_id,
            case recent_days
                when 7 then view_count_7d
                when 30 then view_count_30d
            end view_count
        from dws_traffic_page_visitor_page_view_nd lateral view explode(array(7,30)) tmp as recent_days
        -- 你也可以从dws_traffic_page_visitor_page_view_1d最近一天表求这些数据, 那么你这个dws_traffic_page_visitor_page_view_nd
        -- n天表就没有意义了, 这就是nd表的好处, 在这里体现

        where dt='2020-06-14'
        and page_id in ('home','good_detail')
    )t1
    group by recent_days
) as page
join
(
    select
        1 recent_days,
        count(*) cart_count
    from dws_trade_user_cart_add_1d

    -- 交易域用户粒度加购最近1日汇总事实表
    -- user_id  cart_add_count_1d   cart_add_num_1d     dt
    -- 122      5                   12                  2020-06-10

    where dt='2020-06-14'
    union all
    select
        recent_days,
        sum(if(cart_count>0,1,0))
    from
    (
        select
            recent_days,
            case recent_days
                when 7 then cart_add_count_7d
                when 30 then cart_add_count_30d
            end cart_count
        from dws_trade_user_cart_add_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='2020-06-14'
    )t1
    group by recent_days
) as cart
on page.recent_days=cart.recent_days
join (
    select
        1 recent_days,
        count(*) order_count
    from dws_trade_user_order_1d
    -- 统计了每个用户在最近1天里面下单次数、下单件数、花了多少钱

    where dt='2020-06-14'
    union all
    select
        recent_days,
        sum(if(order_count>0,1,0))
    from
    (
        select
            recent_days,
            case recent_days
                when 7 then order_count_7d
                when 30 then order_count_30d
            end order_count
        from dws_trade_user_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='2020-06-14'
    )t1
    group by recent_days
) as ord
on page.recent_days=ord.recent_days
join (
    select
        1 recent_days,
        count(*) payment_count
    from dws_trade_user_payment_1d
    -- 统计了每个用户在最近1天里面支付聚合详情
    where dt='2020-06-14'
    union all
    select
        recent_days,
        sum(if(order_count>0,1,0))
    from
    (
        select
            recent_days,
            case recent_days
                when 7 then payment_count_7d
                when 30 then payment_count_30d
            end order_count
        from dws_trade_user_payment_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='2020-06-14'
    )t1
    group by recent_days
) as pay
on page.recent_days=pay.recent_days;


-- /******************************************* 7. 商品主题 - 最近7/30日各品牌复购率统计 *************************************************/
-- 统计周期	    统计粒度	指标	    说明
-- 最近7、30日	品牌	    复购率	    重复购买人数占购买人数比例

-- 咱们这里的购买 -> 下单即为购买 -> 不管有没有支付 -> 复购 -> 重复购买

-- 假设有10个品牌, 那么咱们每天就会往表里插入20条数据
insert overwrite table ads_repeat_purchase_by_tm
select * from ads_repeat_purchase_by_tm
union
select
    '2020-06-14' dt,        -- '统计日期'
    recent_days,            -- '最近天数,7:最近7天,30:最近30天'
    tm_id,                  -- '品牌ID'
    tm_name,                -- '品牌名称'
    sum(if(order_count>=2,1,0))/sum(if(order_count>=1,1,0)) -- '复购率'
    -- sum(if(order_count>=1,1,0))
    -- 这里七天可能购买量是0, 所以sum(if(order_count>=1,1,0))不能省略为sum(order_count)
    -- 下单次数 >= 2 就是所谓的重复购买
from
(
    select
        '2020-06-14' dt,
        recent_days,
        user_id,
        tm_id,
        tm_name,
        sum(order_count) order_count
        -- 一段时间内每个用户购买每个品牌的次数
        -- 不需要sum(if)是因为, 这里不统计个数, 只统计总和, 你7天没有下单, 加和0没有任何关系
    from
    (
        select
            recent_days,
            user_id,
            tm_id,
            tm_name,
            case recent_days
                when 7 then order_count_7d
                when 30 then order_count_30d
            end order_count
        from dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
        -- 最近n天用户商品粒度汇总表
        where dt='2020-06-14'
    )t1
    group by recent_days,user_id,tm_id,tm_name
)t2
group by recent_days,tm_id,tm_name;

-- /***************************************** 8. 商品主题 - 各分类商品购物车存量Top10统计 ***********************************************/

-- A商品购物车存量
-- 1001 购物车中A商品存量为10
-- 1002 购物车中A商品存量为12

-- A商品购物车存量为所有用户加和 10+12

-- 找出各分类下面排行前10的商品

insert overwrite table ads_sku_cart_num_top3_by_cate
select * from ads_sku_cart_num_top3_by_cate
union
select
    '2020-06-14' dt,        -- '统计日期'
    category1_id,           -- '一级分类ID'
    category1_name,         -- '一级分类名称'
    category2_id,           -- '二级分类ID'
    category2_name,         -- '二级分类名称'
    category3_id,           -- '三级分类ID'
    category3_name,         -- '三级分类名称'
    sku_id,                 -- '商品id'
    sku_name,               -- '商品名称'
    cart_num,               -- '购物车中商品数量'
    rk                      -- '排名'
from
(
    select
        sku_id,
        sku_name,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        cart_num,
        rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
    from
    (
        select
            sku_id,
            sum(sku_num) cart_num
        from dwd_trade_cart_full
        -- 存量型指标 -> 周期快照事实表 -> 交易域购物车周期快照事实表
        -- 目前某个购物车中某个用户加购某个商品的件数
        where dt='2020-06-14'
        group by sku_id
        -- 把所有人的购物车中的这个商品存量加一起就能得到这个商品的购物车存量
    )cart
    left join
    (
        select
            id,
            sku_name,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name
        from dim_sku_full
        where dt='2020-06-14'
    )sku
    on cart.sku_id=sku.id
)t1
where rk<=3;



-- /***************************************** 9. 交易主题 - 各省份交易统计 ***********************************************/
-- 统计周期	        统计粒度	指标	    说明
-- 最近1、7、30日	省份	    订单数	    略
-- 最近1、7、30日	省份	    订单金额	略

-- 34 * 3条数据 -> 每天
insert overwrite table ads_order_by_province
select * from ads_order_by_province
union
-- 最近一天直接拿
select
    '2020-06-14' dt,        -- '统计日期'
    1 recent_days,          -- '最近天数,1:最近1天,7:最近7天,30:最近30天'
    province_id,            -- '省份ID'
    province_name,          -- '省份名称'
    area_code,              -- '地区编码'
    iso_code,               -- '国际标准地区编码'
    iso_3166_2,             -- '国际标准地区编码'
    order_count_1d,         -- '订单数'
    order_total_amount_1d   -- '订单金额'
from dws_trade_province_order_1d
where dt='2020-06-14'
union all
-- 最近n天
select
    '2020-06-14' dt,
    recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    case recent_days
        when 7 then order_count_7d
        when 30 then order_count_30d
    end order_count,
    case recent_days
        when 7 then order_total_amount_7d
        when 30 then order_total_amount_30d
    end order_total_amount
from dws_trade_province_order_nd lateral view explode(array(7,30)) tmp as recent_days
where dt='2020-06-14';