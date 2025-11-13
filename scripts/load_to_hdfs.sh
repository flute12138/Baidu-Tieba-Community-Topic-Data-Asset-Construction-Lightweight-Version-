#!/bin/bash

# 创建HDFS目录
hdfs dfs -mkdir -p /data/ods/baidu_tieba/user_info
hdfs dfs -mkdir -p /data/ods/baidu_tieba/post_info
hdfs dfs -mkdir -p /data/ods/baidu_tieba/comment_info

# 上传数据
hdfs dfs -put -f ../data/user_info.csv /data/ods/baidu_tieba/user_info/
hdfs dfs -put -f ../data/post_info.csv /data/ods/baidu_tieba/post_info/
hdfs dfs -put -f ../data/comment_info.csv /data/ods/baidu_tieba/comment_info/
