-- 一、拉链表处理方法一
insert overwrite table dim_user_zip partition(dt)
select
    id,
    create_time,
    operate_time,
    start_date,
    -- 6. 旧信息闭链处理, 时间为当天的前一天
    if (rk = 2, date_sub('2020-06-15', 1), end_date) end_date,
    -- 7. 动态分区字段为end_date字段
    if (rk = 2, date_sub('2020-06-15', 1), end_date) dt
from (
    select
        id,
        create_time,
        operate_time,
        start_date,
        end_date,
        -- 5. 将用户最新的开链信息与拉链表最新分区的信息进行排序, 目的是在下一层将排序为第二的旧信息闭链处理
        row_number() over(partition by id order by start_date desc) as rk
    from (
        -- 3. 选择维度层的'9999-12-31'最新分区的用户拉链表信息 
        select
            id,
            create_time,
            operate_time,
            start_date,
            end_date
        from dim_user_zip
        where dt = '9999-12-31'

        -- 4. 将拉链表最新分区的用户信息与当天新增及修改的用户进行 union
        union
        
        -- 2. 将新增及修改的用户信息作开链处理, 开始时间是当天, 闭链时间是'9999-12-31'
        select
            id,
            create_time,
            operate_time,
            '2020-06-15' start_date,
            '9999-12-31' end_date
        from (
            -- 1. 用户表 一天中某个用户信息可能会被捕捉到很多条, 按照用户id分区, 按照时间戳降序排列, 取第一条
            select
                id,
                create_time,
                operate_time,
                row_number() over (partition by id order by ts desc) rk
            from ods_user_info_inc
            where dt = '2020-06-15' 
        ) as temp_1
        where rk = 1
    ) as temp_2
) as temp_3

-- 二、拉链表处理方法二
-- 1. 创建用户拉链临时表, 字段与原拉链表一样
drop table if exists dim_user_zip_tmp;
create external table dim_user_zip_tmp (
    id string,
    create_time string,
    operate_time string,
    start_date string,
    end_date string
) comment '用户拉链临时表'
stored as parquet
location '';
-- 2. 往拉链临时表导入数据
insert overwrite table dim_user_zip_tmp
select
    *
from (
    -- 3. 当天新增及修改数据开链处理
    select
        id,
        create_time,
        operate_time,
        '2020-06-15' start_date,
        '9999-12-31' end_date
    from ods_user_info 
    where dt = '2020-06-15'

    -- 5. 两张表信息进行 union
    union all

    select
        id,
        create_time,
        operate_time,
        start_date,
        -- 4. id不为空的, 表示为修改的数据, 对于最新数据要做闭链处理, 修改时间为当天的前一天
        if (
            ou.id is not null and duz.end_date = '9999-12-31',
            date_sub(ou.dt, 1),
            duz.end_date
        ) as end_date
    from dim_user_zip as duz
    left join (
        select
            *
        from ods_user
        where dt = '2020-06-15'
    ) as ou on duz.id = ou.id
)
-- 6. 将用户拉链临时表的数据覆盖用户拉链表的数据
insert overwrite table dim_user_zip
select * from dim_user_zip_tmp;

-- 三、直播间最大同时在线人数
select
    live_id,
    -- 4. 按照直播间分组, 选取最大人数
    max(user_count) max_user_count
from
(
    -- 3. 按照直播间分区, 事件时间正序排序, 作加和, 可以得到到这个时间为止的在线人数
    select
        live_id,
        sum(user_change) over(partition by live_id order by event_time) user_count
    from
    (
        -- 1. 进入直播间时间
        select user_id,
               live_id,
               in_datetime event_time,
               1 user_change
        from live_events

        union all

        -- 2. 离开直播间时间
        select user_id,
               live_id,
               out_datetime event_time,
               -1 user_change
        from live_events
    ) as temp_1
) as temp_2
group by live_id;

-- 四、页面浏览会话划分
-- user_id	    page_id	    view_timestamp
-- 100	        home	    1659950435
select 
    user_id,
    page_id,
    view_timestamp,
    -- 3. 会话标记规则 - 用户id + 按照用户分区浏览时间正序排列加和的结果 -> 这样可以区分每个回话
    concat(user_id, '-', sum(session_start_point) over (partition by user_id order by view_timestamp)) session_id
from (
    select 
        user_id,
        page_id,
        view_timestamp,
        -- 2. 本次浏览时间减去上次浏览时间超过60s, 视为新会话
        if(view_timestamp - lagts >= 60, 1, 0) session_start_point
    from (
        select 
            user_id,
            page_id,
            view_timestamp,
            -- 1. 按照用户分区, 按照浏览时间正序排列, 选出本次浏览的上次浏览时间
            lag(view_timestamp, 1, 0) over (partition by user_id order by view_timestamp) lagts
        from page_view_events
    ) temp_1
) temp_2;

-- 五、用户间隔连续登录
-- user_id	login_datetime
-- 100	    2021-12-01 19:00:00
select
    user_id,
    -- 6. 按照用户分组, 求出每个用户最大的连续天数
    max(recent_days) max_recent_days  
from
(
    select
        user_id,
        user_flag,
        -- 5. 按照各分组求每个用户每次连续的天数(记得加1)
        datediff(max(login_date),min(login_date)) + 1 recent_days 
    from
    (
        select
            user_id,
            login_date,
            lag1_date,
            -- 4. 拼接 用户和标签 分组
            concat(user_id,'_',flag) user_flag
        from
        (
            select
                user_id,
                login_date,
                lag1_date,
                -- 3. 获取大于2的标签
                sum(if(datediff(login_date,lag1_date)>2,1,0)) over(partition by user_id order by login_date) flag  
            from
            (
                select
                    user_id,
                    login_date,
                    -- 2. 获取上一次登录日期
                    lag(login_date,1,'1970-01-01') over(partition by user_id order by login_date) lag1_date  
                from
                (
                    -- 1. 按照用户和日期去重
                    select
                        user_id,
                        date_format(login_datetime,'yyyy-MM-dd') login_date
                    from login_events
                    group by user_id,date_format(login_datetime,'yyyy-MM-dd')  
                )t1
            )t2
        )t3
    )t4
    group by user_id,user_flag
)t5
group by user_id;

-- 六、日期交叉问题: 求每个品牌的优惠总天数
-- (一个品牌在同一天可能有多个优惠活动, 但是只能算一天)
-- promotion_id	    brand	start_date	end_date
-- 1	            oppo	2021-06-05	2021-06-09


-- brand    start   end
-- huawei   10.1    10.7
-- huawei   10.10   10.17
-- huawei   10.15   10.17


-- 思路一
-- 每一行end-start, 然后将所有的加和 -> 这种思路明显不对, 如果有包含的, 天数就会算多

-- 思路二
-- 最大的end与最小的start作差 -> 这种思路也不对, 如果有间断, 天数也会算多

-- 思路三
-- (8, 'huawei', '2021-06-05', '2021-06-26'),
-- (9, 'huawei', '2021-06-09', '2021-06-15'),
-- (10, 'huawei', '2021-06-17', '2021-06-21');

-- start_date
-- 每次这个开始日期, 都拿按照brand和end_date正序, 从第一条到本记录前一条的最大结束时间date, 
-- 如果这个date比当前开始时间小, 那么存在间断, 开始时间不变
-- 如果这个date比当前开始时间大, 那么存在包含, 开始时间变为(date + 1)
    1. 如果开始时间变为(date + 1)后比本记录的结束时间还大, 说明本条记录被全包含, 那么就被过滤掉了
    2. 所以只需要拿(date + 1) <= 本条记录结束时间的数据

select
    brand,
    sum(datediff(end_date,start_date) + 1) promotion_day_count
from
(
    select
        brand,
        max_end_date,
        if(
            -- 1. 如果为空, 说明是第一行, 那么开始日期就不变
            max_end_date is null 
            -- 2. 最近一次活动时间比本次活动的开始时间小, 说明有隔断, 开始日期仍不变
            or start_date > max_end_date,
            start_date,
            -- 3. 本次开始时间 <= 最近一次活动的开始时间, 说明一定有交叉, 
            --    那么就把此次开始时间变成, 最近一次活动结束时间的后一天
            date_add(max_end_date,1)
        ) start_date,
        end_date
    from
    (
        select
            brand,
            start_date,
            end_date,
            -- 1. 按照品牌分区, 取最近一次的结束日期
            max(end_date) over(partition by brand order by start_date rows between unbounded preceding and 1 preceding) max_end_date
        from promotion_info
    )t1
)t2
-- 4. start_date 比 end_date 还大的就要被pass掉 
where end_date >= start_date
group by brand;