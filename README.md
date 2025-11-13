🏗️ Baidu-Tieba-Community-Topic-Data-Asset-Construction (轻量版)

百度贴吧社区主题数据资产建设项目（轻量可运行版）

📘 一、项目简介

该项目模拟“百度贴吧”社区论坛主题数据资产建设的完整流程，
从 数据采集 → 清洗 → 汇总 → 应用层分析 → 结果导出，
旨在展示轻量级数仓的构建与指标体系搭建过程。

可在个人 Hadoop + Hive 环境中一键运行。

📁 二、项目目录结构
/root/baidu_tieba_dw/
│
├── data/                      # 模拟原始数据
│   ├── gen_data.py            # 生成样例CSV数据
│   ├── user_info.csv
│   ├── post_info.csv
│   └── comment_info.csv
│
├── scripts/                   # Hive & Shell脚本
│   ├── load_to_hdfs.sh        # 上传数据至HDFS
│   ├── hive_create_all.sql    # 建立ODS层表
│   ├── hive_dwd_clean.sql     # 清洗并生成DWD层
│   ├── hive_dws_summary.sql   # 汇总生成DWS层
│   ├── hive_ads_analysis.sql  # 输出ADS层指标
│
└── README.txt


📌 创建命令：

mkdir -p ~/baidu_tieba_dw/{data,scripts}
cd ~/baidu_tieba_dw

⚙️ 三、环境要求
组件	要求版本	检查命令
Hadoop	3.x	hadoop version
HDFS	已启动	hdfs dfs -ls /
Hive	3.x	hive --version
Python	≥3.7	python3 --version
🧱 四、数据生成（~/baidu_tieba_dw/data/gen_data.py）
import random, csv
from datetime import datetime, timedelta

users = [f"user_{i}" for i in range(1, 51)]
forums = ["AI吧", "编程吧", "游戏吧", "考研吧"]

# --- 用户表 ---
with open("user_info.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["user_id", "register_time", "user_level"])
    for u in users:
        writer.writerow([u, (datetime.now() - timedelta(days=random.randint(0, 600))).strftime("%Y-%m-%d"), random.randint(1, 10)])

# --- 帖子表 ---
with open("post_info.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["post_id", "title", "user_id", "forum_name", "create_time", "reply_count", "like_count"])
    for i in range(1, 501):
        writer.writerow([
            i,
            f"帖子标题_{i}",
            random.choice(users),
            random.choice(forums),
            (datetime.now() - timedelta(days=random.randint(0, 90))).strftime("%Y-%m-%d %H:%M:%S"),
            random.randint(0, 100),
            random.randint(0, 500)
        ])

# --- 评论表 ---
with open("comment_info.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["comment_id", "post_id", "user_id", "comment_content", "comment_time"])
    for i in range(1, 1001):
        writer.writerow([
            i,
            random.randint(1, 500),
            random.choice(users),
            random.choice(["不错", "支持", "学习了", "顶", "水贴", "广告", "哈哈"]),
            (datetime.now() - timedelta(days=random.randint(0, 90))).strftime("%Y-%m-%d %H:%M:%S")
        ])


📌 运行命令：

cd ~/baidu_tieba_dw/data
python3 gen_data.py


✅ 生成文件：

user_info.csv
post_info.csv
comment_info.csv

🚚 五、数据导入（~/baidu_tieba_dw/scripts/load_to_hdfs.sh）
#!/bin/bash

# 创建HDFS目录
hdfs dfs -mkdir -p /data/ods/baidu_tieba/user_info
hdfs dfs -mkdir -p /data/ods/baidu_tieba/post_info
hdfs dfs -mkdir -p /data/ods/baidu_tieba/comment_info

# 上传数据
hdfs dfs -put -f ../data/user_info.csv /data/ods/baidu_tieba/user_info/
hdfs dfs -put -f ../data/post_info.csv /data/ods/baidu_tieba/post_info/
hdfs dfs -put -f ../data/comment_info.csv /data/ods/baidu_tieba/comment_info/


📌 运行命令：

cd ~/baidu_tieba_dw/scripts
bash load_to_hdfs.sh


✅ 验证上传：

hdfs dfs -ls /data/ods/baidu_tieba/user_info/

🧮 六、ODS 层（~/baidu_tieba_dw/scripts/hive_create_all.sql）
-- 用户表
CREATE EXTERNAL TABLE IF NOT EXISTS ods_user_info (
  user_id STRING,
  register_time STRING,
  user_level INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/data/ods/baidu_tieba/user_info/';

-- 帖子表
CREATE EXTERNAL TABLE IF NOT EXISTS ods_post_info (
  post_id INT,
  title STRING,
  user_id STRING,
  forum_name STRING,
  create_time STRING,
  reply_count INT,
  like_count INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/data/ods/baidu_tieba/post_info/';

-- 评论表
CREATE EXTERNAL TABLE IF NOT EXISTS ods_comment_info (
  comment_id INT,
  post_id INT,
  user_id STRING,
  comment_content STRING,
  comment_time STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/data/ods/baidu_tieba/comment_info/';


📌 执行命令：

hive -f ~/baidu_tieba_dw/scripts/hive_create_all.sql


✅ 检查：

SHOW TABLES LIKE 'ods_%';
SELECT * FROM ods_post_info LIMIT 5;

🧹 七、DWD 清洗层（~/baidu_tieba_dw/scripts/hive_dwd_clean.sql）
-- 清洗帖子表
CREATE TABLE dwd_post_detail AS
SELECT
  post_id,
  title,
  user_id,
  forum_name,
  from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')) AS create_time,
  IF(reply_count < 0, 0, reply_count) AS reply_count,
  IF(like_count < 0, 0, like_count) AS like_count
FROM ods_post_info
WHERE title IS NOT NULL AND user_id IS NOT NULL;

-- 清洗评论表（去除广告、水贴）
CREATE TABLE dwd_comment_detail AS
SELECT
  comment_id,
  post_id,
  user_id,
  comment_content,
  from_unixtime(unix_timestamp(comment_time, 'yyyy-MM-dd HH:mm:ss')) AS comment_time
FROM ods_comment_info
WHERE comment_content NOT RLIKE '广告|水贴|政治|违法';

-- 清洗用户表
CREATE TABLE dwd_user_info AS
SELECT
  user_id,
  from_unixtime(unix_timestamp(register_time, 'yyyy-MM-dd')) AS register_time,
  user_level
FROM ods_user_info
WHERE user_id IS NOT NULL;


📌 执行命令：

hive -f ~/baidu_tieba_dw/scripts/hive_dwd_clean.sql


✅ 检查：

SHOW TABLES LIKE 'dwd_%';
SELECT COUNT(*) FROM dwd_comment_detail;

📊 八、DWS 汇总层（~/baidu_tieba_dw/scripts/hive_dws_summary.sql）
-- 用户活跃指标
CREATE TABLE dws_user_active AS
SELECT
  u.user_id,
  COUNT(DISTINCT p.post_id) AS post_count,
  SUM(p.reply_count) AS total_replies,
  SUM(p.like_count) AS total_likes
FROM dwd_user_info u
LEFT JOIN dwd_post_detail p ON u.user_id = p.user_id
GROUP BY u.user_id;

-- 贴吧热度指标
CREATE TABLE dws_forum_hot_rank AS
SELECT
  forum_name,
  COUNT(post_id) AS post_num,
  SUM(reply_count + like_count) AS activity_score
FROM dwd_post_detail
GROUP BY forum_name;

-- 近30天发帖趋势
CREATE TABLE dws_post_trend AS
SELECT
  date_format(create_time,'yyyy-MM-dd') AS day,
  COUNT(*) AS post_count
FROM dwd_post_detail
WHERE create_time >= date_sub(current_date(),30)
GROUP BY date_format(create_time,'yyyy-MM-dd');


📌 执行命令：

hive -f ~/baidu_tieba_dw/scripts/hive_dws_summary.sql


✅ 检查：

SELECT * FROM dws_forum_hot_rank;

🧠 九、ADS 应用层（~/baidu_tieba_dw/scripts/hive_ads_analysis.sql）
-- 用户分层标签
CREATE TABLE ads_user_tag AS
SELECT
  user_id,
  CASE 
    WHEN total_replies + total_likes > 400 THEN '高活跃'
    WHEN total_replies + total_likes BETWEEN 100 AND 400 THEN '中活跃'
    ELSE '低活跃'
  END AS user_tag
FROM dws_user_active;

-- 热帖榜Top10
CREATE TABLE ads_hot_post AS
SELECT
  post_id,
  title,
  user_id,
  forum_name,
  (reply_count + like_count) AS hot_score
FROM dwd_post_detail
ORDER BY hot_score DESC
LIMIT 10;

-- 各贴吧综合排名
CREATE TABLE ads_forum_rank AS
SELECT
  forum_name,
  post_num,
  activity_score,
  RANK() OVER (ORDER BY activity_score DESC) AS rank_id
FROM dws_forum_hot_rank;


📌 执行命令：

hive -f ~/baidu_tieba_dw/scripts/hive_ads_analysis.sql


✅ 检查：

SELECT * FROM ads_user_tag LIMIT 10;
SELECT * FROM ads_hot_post LIMIT 10;
SELECT * FROM ads_forum_rank LIMIT 10;

📤 十、结果导出
hive -e "SELECT * FROM ads_hot_post;" > ~/baidu_tieba_dw/hot_post_result.csv

🚀 十一、执行顺序总结表
阶段	命令 / 文件	说明
1️⃣	python3 gen_data.py	生成模拟CSV数据
2️⃣	bash load_to_hdfs.sh	导入HDFS
3️⃣	hive -f hive_create_all.sql	建立ODS层外部表
4️⃣	hive -f hive_dwd_clean.sql	数据清洗与标准化
5️⃣	hive -f hive_dws_summary.sql	汇总与指标统计
6️⃣	hive -f hive_ads_analysis.sql	生成应用层分析结果
7️⃣	hive -e ... > csv	导出结果为CSV文件
📈 十二、最终输出结果
表名	内容说明
ads_user_tag	用户分层标签
ads_hot_post	热帖榜Top10
ads_forum_rank	各贴吧活跃度排行
💡 十三、后续扩展建议
场景	建议
可视化展示	可导入 Superset / FineBI 实现 BI 展示
数据量扩大	转为 ORC/Parquet 格式
自动化调度	可引入 Airflow / Azkaban 实现调度
学习拓展	增加“用户留存分析”“帖子生命周期分析”等模块
