# -*- coding:utf-8 -*-
__Author__ = "KrianJ wj_19"
__Time__ = "2020/7/28 22:15"
__doc__ = """ """

import pandas as pd
import numpy as np
import jieba

def get_stopwords():
    """去除标点符号
    :param data: 分词之后句子的集合 <class ndarray>
    :return 去除标点符号之后的集合"""
    # 创建停用词表
    marks = r"1234567890[\s+\.\!\/_,$%^*())?;；:-【】+\"\']+|[+——！！，;:。？、~@#￥%……&*（）]+ ，。"
    stopwords = []
    f = open(r'D:\Pyproject\scrapy_stuff\analysis\tech_china\collected_data\stopwords.txt', 'r', encoding='utf-8')
    line = f.readline()[:-1]
    while line:
        stopwords.append(line)
        line = f.readline()[:-1]
    f.close()
    all_stopwords = list(marks) + stopwords
    return all_stopwords


def partition_remove(series: pd.Series):
    """对DataFrame给定字段分词并去停用词"""
    texts = np.array([jieba.lcut(text) for text in series])
    stopwords = get_stopwords()
    for i in range(len(texts)):
        text = texts[i]
        new_text = [ele for ele in text if ele not in stopwords]
        texts[i] = ' '.join(new_text)
    new_series = pd.Series(texts)
    return new_series


if __name__ == '__main__':
    print(get_stopwords())
