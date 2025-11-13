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

