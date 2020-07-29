# -*- coding:utf-8 -*-
__Author__ = "KrianJ wj_19"
__Time__ = "2020/7/28 23:27"
__doc__ = """ 生成词云"""

import wordcloud
import pandas as pd
import imageio


def gen_cloud(read_path, store_path, mask_path):
    """读取csv文件，生成词云"""
    mask = imageio.imread(mask_path)
    data = pd.read_csv(read_path)
    # 获取全部text字段文本，组合成字符串
    long_text = ' '.join(list(data.values[:, 1]))
    # 生成词云
    wc = wordcloud.WordCloud(
        width=1198,
        height=1000,
        background_color='white',
        font_path='msyh.ttc',
        mask=mask
    )
    wc.generate(long_text)
    wc.to_file(store_path)
    return None


if __name__ == '__main__':
    tech_collections = ['articles', 'industrys', 'internet', 'telphone']
    digi_collections = ['cameradv', 'notebookpc', 'smartphone', 'tabletpc']
    # 生成每个collection的所有text的词云
    for collection in digi_collections+tech_collections:
        if collection in tech_collections:
            csv_path = r'D:\Pyproject\scrapy_stuff\analysis\tech_china\collected_data\china_tech\%s.csv' % collection
        else:
            csv_path = r'D:\Pyproject\scrapy_stuff\analysis\tech_china\collected_data\china_digi\%s.csv' % collection
        mask_path = r'D:\Pyproject\scrapy_stuff\analysis\tech_china\collected_data\wordclouds\mask\chinamap.png'
        png_path = r'D:\Pyproject\scrapy_stuff\analysis\tech_china\collected_data\wordclouds\%s.png' % collection
        gen_cloud(csv_path, png_path, mask_path)
