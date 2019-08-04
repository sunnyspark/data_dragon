/*
广告分析主题ads层：广告概况报表 ads_ad_overview

结构：
  adis	曝光次数  曝光人数    曝光次数最大页   点击次数   点击人数  点击次数最大页


@Author HUNTER
@Date 2019-07-26

@源表：dwd_ad_event_dtl  广告事件明细表
@目标：ads_ad_overview  广告概况报表

@计算逻辑：
1,先抽取广告点击、曝光事件数据

+---------+------------------------+-------+------------+
|   uid   |          url           | adid  | eventtype  |
+---------+------------------------+-------+------------+
| LXOGIR  | http://www.51doit.com  | 1     | ad_show    |
| LXOGIR  | http://www.51doit.com  | 2     | ad_show    |
| LXOGIR  | http://www.51doit.com  | 3     | ad_show    |
| LXOGIR  | http://www.51doit.com  | 4     | ad_show    |
| LXOGIR  | http://www.51doit.com  | 4     | ad_click    |
| LXOGIR  | http://www.51doit.com  | 6     | ad_show    |
| NNVGIZ  | http://www.51doit.com  | 1     | ad_show    |
| NNVGIZ  | http://www.51doit.com  | 2     | ad_show    |
| NNVGIZ  | http://www.51doit.com  | 2     | ad_click    |
| NNVGIZ  | http://www.51doit.com  | 4     | ad_show    |
+---------+------------------------+-------+------------+


2, 以页面和adid为根据，算曝光次数，曝光人数
+----------------------------------------+---------+--------------+--------------------+
|                 a.url                  | a.adid  | a.show_cnts  | a.show_users_cnts  |
+----------------------------------------+---------+--------------+--------------------+
| http://www.51doit.com                  | 1       | 1307         | 1016               |
| http://www.51doit.com                  | 2       | 1101         | 893                |
| http://www.51doit.com                  | 3       | 878          | 743                |
| http://www.51doit.com                  | 4       | 646          | 567                |
| http://www.51doit.com                  | 5       | 422          | 391                |
| http://www.51doit.com                  | 6       | 196          | 192                |
| https://www.51doit.com/ad/beiyaoqing/  | 1       | 2            | 2                  |
| https://www.51doit.com/ad/beiyaoqing/  | 2       | 2            | 2                  |
| https://www.51doit.com/ad/beiyaoqing/  | 3       | 2            | 2                  |
| https://www.51doit.com/ad/xunbao/      | 1       | 2            | 2                  |
+----------------------------------------+---------+--------------+--------------------+

2.1, 以adid分窗口，按曝光次数倒序排序，为每条数据附加： 曝光次数最大页
| http://www.51doit.com                  | 1     | 1307  | 1016  | http://www.51doit.com
| https://www.51doit.com/ad/beiyaoqing/  | 1     | 2     | 2     | http://www.51doit.com
| https://www.51doit.com/ad/xunbao/      | 1     | 2     | 2     | http://www.51doit.com
| http://www.51doit.com                  | 2     | 1101  | 893   | http://www.51doit.com
| https://www.51doit.com/ad/beiyaoqing/  | 2     | 2     | 2     | http://www.51doit.com

2.2, 以adid分组，对次数sum，对人数sum，对曝光次数最大页求max  -- 变成了一个广告，一条数据
+-------+------------+------------------+------------------------+
| adid  | show_cnts  | show_users_cnts  |        url_max         |
+-------+------------+------------------+------------------------+
| 1     | 2352       | 2061             | http://www.51doit.com  |
| 2     | 1957       | 1749             | http://www.51doit.com  |
| 3     | 1566       | 1431             | http://www.51doit.com  |
| 4     | 1142       | 1063             | http://www.51doit.com  |
| 5     | 756        | 725              | http://www.51doit.com  |
| 6     | 373        | 369              | http://www.51doit.com  |
+-------+------------+------------------+------------------------+



3, 以页面和adid为根据，算点击次数，点击人数
3.1, 以adid分窗口，按点击次数倒序排序，为每条数据附加： 点击次数最大页
3.2, 以adid分组，对次数sum，对人数sum，对点击次数最大页求max  -- 变成了一个广告，一条数据

4.将上述两步的结果进行left join

 */
-- 建表
create table ads_ad_overview(
dt string,
adid string,
show_cnts int,
show_user_cnts int,
show_max_url string,

click_cnts int,
click_user_cnts int,
click_max_url string
)
stored as parquet
;



-- etl 计算
WITH tmp1
AS (
    SELECT uid
        ,event ['url'] AS url
        ,event ['ad_id'] AS adid
        ,eventtype
    FROM dwd_ad_event_dtl
    WHERE eventtype = 'ad_show'
    )
    ,tmp2
AS (
    SELECT uid
        ,event ['url'] AS url
        ,event ['ad_id'] AS adid
        ,eventtype
    FROM dwd_ad_event_dtl
    WHERE eventtype = 'ad_click'
    )



INSERT INTO TABLE ads_ad_overview

SELECT
'2019-06-16' as dtstr,
o1.adid,
o1.show_cnts,
o1.show_users_cnts,
o1.url_max,
o2.click_cnts,
o2.click_users_cnts,
o2.url_max

FROM (
    SELECT adid
        ,sum(show_cnts) AS show_cnts
        ,sum(show_users_cnts) AS show_users_cnts
        ,max(url_max) AS url_max
    FROM (
        SELECT url
            ,adid
            ,show_cnts
            ,show_users_cnts
            ,first_value(url) OVER (
                PARTITION BY adid ORDER BY show_cnts DESC
                ) AS url_max
        FROM (
            SELECT url
                ,adid
                ,count(1) AS show_cnts
                ,-- 曝光次数
                count(DISTINCT uid) AS show_users_cnts -- 曝光人数
            FROM tmp1
            GROUP BY url
                ,adid
            ) a
        ) o
    GROUP BY adid
    ) o1





LEFT JOIN



(
    SELECT adid
        ,sum(click_cnts) AS click_cnts
        ,sum(click_users_cnts) AS click_users_cnts
        ,max(url_max) AS url_max
    FROM (
        SELECT url
            ,adid
            ,click_cnts
            ,click_users_cnts
            ,first_value(url) OVER (
                PARTITION BY adid ORDER BY click_cnts DESC
                ) AS url_max
        FROM (
            SELECT url
                ,adid
                ,count(1) AS click_cnts
                ,-- 点击次数
                count(DISTINCT uid) AS click_users_cnts -- 点击人数
            FROM tmp2
            GROUP BY url
                ,adid
            ) a
        ) o
    GROUP BY adid
    ) o2 ON o1.adid = o2.adid;

/*
+----------+---------------+---------------------+------------------------+----------+----------------+----------------------+-------------+
| o1.adid  | o1.show_cnts  | o1.show_users_cnts  |       o1.url_max       | o2.adid  | o2.click_cnts  | o2.click_users_cnts  | o2.url_max  |
+----------+---------------+---------------------+------------------------+----------+----------------+----------------------+-------------+
| 1        | 2352          | 2061                | http://www.51doit.com  | NULL     | NULL           | NULL                 | NULL        |
| 2        | 1957          | 1749                | http://www.51doit.com  | NULL     | NULL           | NULL                 | NULL        |
| 3        | 1566          | 1431                | http://www.51doit.com  | NULL     | NULL           | NULL                 | NULL        |
| 4        | 1142          | 1063                | http://www.51doit.com  | NULL     | NULL           | NULL                 | NULL        |
| 5        | 756           | 725                 | http://www.51doit.com  | NULL     | NULL           | NULL                 | NULL        |
| 6        | 373           | 369                 | http://www.51doit.com  | NULL     | NULL           | NULL                 | NULL        |
+----------+---------------+---------------------+------------------------+----------+----------------+----------------------+-------------+




 */