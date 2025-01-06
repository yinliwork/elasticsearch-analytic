import pandas as pd
import re
from paddlenlp.transformers import BertTokenizer, BertForSequenceClassification
import paddle
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import jieba.analyse
from typing import Tuple, List

# 加载分词器和模型
tokenizer = BertTokenizer.from_pretrained("bert-base-chinese")
model = BertForSequenceClassification.from_pretrained("bert-base-chinese", num_classes=2) # 分类
data = pd.read_csv("2_creator_contents_2024-08-29.csv")


# 定义过滤函数
def clean_text(text):
    if type(text) == str:
        # 先移除方括号及其内容
        text = re.sub(r'\[.*?\]', '', text)
        # 再移除其他特殊字符
        text = re.sub(r"[^\w\s\u4e00-\u9fff]", "", text)  # 仅保留中文、英文、数字、空格
        text = text.strip()  # 去掉首尾空格
    else:
        text = ""
    return text


def analyze_sentiment(texts):
    # 批量处理文本
    inputs = tokenizer(texts, return_tensors="pd", max_length=128, padding=True, truncation=True)
    logits = model(**inputs)
    predictions = paddle.argmax(logits, axis=1).numpy()
    return ["正面" if p == 1 else "负面" for p in predictions]

def analyze_keywords(text: str, top_k: int = 3) -> str:
    """
    提取文本中的关键词
    Args:
        text: 输入文本
        top_k: 返回前k个关键词
    Returns:
        关键词字符串，以逗号分隔
    """
    if not text or not isinstance(text, str):
        return ""
    
    try:
        # 使用 jieba TF-IDF 提取关键词
        keywords = jieba.analyse.extract_tags(text, topK=top_k)
        return ",".join(keywords)
    except Exception as e:
        print(f"关键词提取错误: {e}")
        return ""

def analyze_text(texts: List[str]) -> Tuple[List[str], List[str]]:
    """
    同时进行情感分析和关键词提取
    """
    # 情感分析
    inputs = tokenizer(texts, return_tensors="pd", max_length=128, padding=True, truncation=True)
    logits = model(**inputs)
    predictions = paddle.argmax(logits, axis=1).numpy()
    # 自定义标签映射
    # label_map = {0: "负面", 1: "中立", 2: "正面"}
    # sentiments = [label_map[pred] for pred in predictions]
    sentiments = ["正面" if p == 1 else "负面" for p in paddle.argmax(logits, axis=1).numpy()]
    
    # 关键词提取
    keywords = [analyze_keywords(text) for text in texts]
    
    return sentiments, keywords

def process_batch(batch_texts):
    # 清理文本
    cleaned_texts = [clean_text(text) for text in batch_texts]
    # 分析情感和关键词
    sentiments, keywords = analyze_text(cleaned_texts)
    return sentiments, keywords

def process_data_in_batches(data, batch_size=1000, max_workers=4):
    sentiment_results = []
    keyword_results = []
    
    # 将数据分成多个批次
    total_rows = len(data)
    batches = np.array_split(data['desc'], np.ceil(total_rows/batch_size))
    
    # 使用线程池处理批次，并保存每个批次的索引
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务并保存 Future 对象
        for batch in batches:
            future = executor.submit(process_batch, batch)
            futures.append(future)
        
        # 按提交顺序获取结果
        for future in futures:
            batch_sentiments, batch_keywords = future.result()
            sentiment_results.extend(batch_sentiments)
            keyword_results.extend(batch_keywords)

    return sentiment_results, keyword_results

# 主处理流程
batch_size = 100  # 每批处理的数据量
max_workers = 2   # 线程数

# 分批处理数据
sentiments, keywords = process_data_in_batches(data, batch_size=batch_size, max_workers=max_workers)

# 将结果添加到 DataFrame
data['sentiment'] = sentiments
data['keywords'] = keywords

# 保存分析结果到新 CSV 文件
# output_file = "comments_with_analysis.csv"
output_file = "content_with_analysis.csv"
data.to_csv(output_file, index=False, encoding="utf-8")
print(f"分析结果已保存到 {output_file}")
