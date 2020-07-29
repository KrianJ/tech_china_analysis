# -*- coding:utf-8 -*-
__Author__ = "KrianJ wj_19"
__Time__ = "2020/7/28 19:18"
__doc__ = """ 对china_tech中的数据库做清洗"""

from data_process.china_tech import connect_mongo, read_mongo, deduplicate
from data_process.stopwords import partition_remove
from data_process.config import *


def process_data(df):
    # 对text字段分词并去停用词
    new_series = partition_remove(df['text'])
    df['text'] = new_series
    # 对title字段分词并去停用词
    new_title = partition_remove(df['title'])
    df['title'] = new_title
    # 对记录进行标注
    df['type'].replace('cameradv', 0, inplace=True)
    df['type'].replace('notebookpc', 1, inplace=True)
    df['type'].replace('smartphone', 2, inplace=True)
    df['type'].replace('tabletpc', 3, inplace=True)
    return df


if __name__ == '__main__':
    # 获取指定数据库
    db = connect_mongo(MONGO_URI, MONGO_PORT, MONGO_DB[1])
    # 获取所有集合并写入DataFrame
    collections = db.list_collection_names()
    for collection in collections:
        # 去重
        # deduplicate(db, collection)
        # 处理每个集合数据并写入csv
        col_df = read_mongo(db, collection)
        process_df = process_data(col_df)
        csv_path = r'D:\Pyproject\scrapy_stuff\analysis\tech_china\collected_data\china_digi\%s.csv' % collection
        process_df.to_csv(csv_path, index=False)

