# -*- coding:utf-8 -*-
__Author__ = "KrianJ wj_19"
__Time__ = "2020/7/28 19:18"
__doc__ = """ 对china_tech中的数据库做清洗"""

import pandas as pd
import pymongo
from data_process.config import *
from data_process.stopwords import partition_remove


def connect_mongo(host, port, db, username=None, password=None):
    """读取到指定mongo的数据库"""
    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        client = pymongo.MongoClient(mongo_uri)
    else:
        client = pymongo.MongoClient(host, port)
    return client[db]


def deduplicate(db, collection_name):
    """对指定集合去重，返回"""
    collection = db[collection_name]
    patent = []
    for patent_record in collection.find({'_id': {'$ne': 0}}):
        if patent_record['url'] not in patent:
            patent.append(patent_record['url'])
        else:
            collection.delete_one({'url': patent_record['url']})
    return None


def read_mongo(db, collection, query={}):
    """对指定集合进行读取并存储至DateFrame"""
    cursor = db[collection].find(query)
    # 读取数据并存储到DataFrame
    df = pd.DataFrame(list(cursor))
    # 删除不需要的字段，增加type字段用于分类
    df = df.drop(columns=['_id', 'url', 'datetime', 'source', 'website'])
    df['type'] = collection
    return df


def process_data(df: pd.DataFrame):
    """分词，去停用词，标注"""
    # 对text字段分词并去停用词
    new_series = partition_remove(df['text'])
    df['text'] = new_series
    # 对title字段分词并去停用词
    new_title = partition_remove(df['title'])
    df['title'] = new_title
    # 对记录进行标注
    df['type'].replace('articles', 0, inplace=True)
    df['type'].replace('industrys', 1, inplace=True)
    df['type'].replace('internet', 2, inplace=True)
    df['type'].replace('telphone', 3, inplace=True)
    return df


if __name__ == '__main__':
    # 获取指定数据库
    db = connect_mongo(MONGO_URI, MONGO_PORT, MONGO_DB[0])
    # 获取所有集合并写入DataFrame
    collections = db.list_collection_names()
    for collection in collections:
        # 去重
        # deduplicate(db, collection)
        # 处理每个集合数据并写入csv
        col_df = read_mongo(db, collection)
        process_df = process_data(col_df)
        csv_path = r'D:\Pyproject\scrapy_stuff\analysis\tech_china\collected_data\china_tech\%s.csv' % collection
        process_df.to_csv(csv_path, index=False)







