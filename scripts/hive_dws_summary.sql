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
