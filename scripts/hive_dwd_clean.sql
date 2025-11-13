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
