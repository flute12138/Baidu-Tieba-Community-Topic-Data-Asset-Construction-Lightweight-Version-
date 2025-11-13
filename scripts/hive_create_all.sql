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
