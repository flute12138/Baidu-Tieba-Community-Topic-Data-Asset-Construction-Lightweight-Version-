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
