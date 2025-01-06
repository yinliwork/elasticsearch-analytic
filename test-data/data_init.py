from elasticsearch import Elasticsearch
import pandas as pd
from datetime import datetime

# 连接到 Elasticsearch
es = Elasticsearch("http://localhost:9200")  # 根据实际情况修改连接地址

# 创建索引映射
index_name = "xiaohongshu_contents"
mapping = {
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "type": {"type": "keyword"},
            "firstPublished": {"type": "date", "format": "epoch_millis"},
            "lastedPublished": {"type": "date", "format": "epoch_millis"},
            "ipLocation": {"type": "keyword"},
            "noteId": {"type": "keyword"},
            "userId": {"type": "keyword"},
            "content": {"type": "text", "analyzer": "ik_max_word"},  # 使用 IK 分词器
            "url": {"type": "keyword"},
            "nickname": {"type": "keyword"},
            "likeCount": {"type": "integer"},
            "shareCount": {"type": "integer"},
            "collectCount": {"type": "integer"},
            "sentiment": {"type": "keyword"},
            "keywords": {"type": "keyword"}
        }
    }
}

# 创建索引
if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body=mapping)

# 读取 CSV 文件
df = pd.read_csv('content_with_analysis.csv')

def convert_count(value):
    """转换数值，处理带"万"的数字"""
    if pd.isna(value):
        return 0
    
    value = str(value)
    if '万' in value:
        # 移除可能的空格，并将"万"替换为空
        number = float(value.strip().replace('万', ''))
        # 乘以10000转换为实际数值
        return int(number * 10000)
    return int(value)

# 批量导入数据
def bulk_import(df, batch_size=1000):
    actions = []
    for i, row in df.iterrows():
        # 添加操作类型和文档 ID
        action = [
            # 操作描述
            {"index": {"_index": index_name, "_id": row['note_id']}},
            # 文档内容
            {
                "id": row['note_id'],
                "firstPublished": int(row['time']),
                "lastedPublished": int(row['last_update_time']),
                "ipLocation": row['ip_location'] if pd.notna(row['ip_location']) else None,
                "noteId": row['note_id'],
                "userId": row['user_id'],
                "content": str(row['desc']) if pd.notna(row['desc']) else "",
                "url": f"https://www.xiaohongshu.com/discovery/item/{row['note_id']}",
                "nickname": row['nickname'],
                "likeCount": convert_count(row['liked_count']),
                "shareCount": convert_count(row['share_count']),
                "collectCount": convert_count(row['collected_count']),
                "sentiment": row['sentiment'],
                "keywords": row['keywords'].split(',') if pd.notna(row['keywords']) else []
            }
        ]
        # 将操作描述和文档内容分别添加到actions列表
        actions.extend(action)
        
        if len(actions) >= batch_size * 2:  # 因为每个文档占用两个元素
            es.bulk(body=actions)
            actions = []
            print(f"已导入 {i+1} 条数据")
    
    if actions:
        es.bulk(body=actions)
        print(f"完成导入，共 {len(df)} 条数据")

# 执行导入
bulk_import(df)
