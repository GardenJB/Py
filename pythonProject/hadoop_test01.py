from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import re
from konlpy.tag import Okt
from datetime import datetime
import json

# Spark 설정
conf = SparkConf().setAppName("Preprocessing").setMaster("local[*]")  # Spark를 로컬 모드로 실행
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# 전처리 시작
# 파일 불러오기
today_date = datetime.now().strftime('%Y-%m-%d')

# frequent_words.txt 파일을 불러와서 집합(set)으로 저장
with open("frequent_words.txt", 'r', encoding='utf-8') as freq_file:
    frequent_words = set(freq_file.read().splitlines())


# with open(f"article_data_{today_date}_100.json", 'r', encoding='utf-8') as file:
with open(f"article_data_100p_100.json", 'r', encoding='utf-8') as file:
    art_df = json.load(file)

# HDFS에서 데이터 파일을 읽어옵니다.
# 여기서 'hdfs://<HDFS_MASTER_IP>:<HDFS_PORT>/path/to/your/file.json'와 같이 HDFS 경로를 지정해야 합니다.
data_path = f"hdfs://<HDFS_MASTER_IP>:<HDFS_PORT>/path/to/your/file.json"
art_df = spark.read.json(data_path)

# Okt 초기화
okt = Okt()

# 정규표현식 패턴: 한글만 남기고 모두 제거 함수
hangul_pattern = re.compile('[^ㄱ-ㅎㅏ-ㅣ가-힣]+')

# DataFrame을 RDD로 변환
art_rdd = art_df.rdd

# 새로운 JSON 데이터를 담을 리스트 초기화
processed_data = []

# 각 원소에 대해 "title"과 "main" 키를 찾아서 처리
for data in art_rdd.collect():  # collect()는 RDD를 로컬 리스트로 변환
    title_text = data.get("title", "")
    main_text = data.get("main", "")

    # 텍스트 데이터를 Okt를 사용하여 처리
    title_text = hangul_pattern.sub('', title_text)  # 한글만 남기고 모두 제거
    title_tokens = okt.morphs(title_text, stem=True)

    # "title_tokens"에 대해서도 토큰의 길이가 1 이상이고 frequent_words.txt에 없는 경우에만 리스트에 추가
    title_tokens = [token for token in title_tokens if len(token) > 1 and token not in frequent_words]

    # main_text를 문장 단위로 분할하여 리스트로
    main_sentences = main_text.split('.')

    # 각 문장을 Okt를 사용하여 처리하고 리스트에 추가
    main_tokens = []
    for sentence in main_sentences:
        sentence = hangul_pattern.sub('', sentence)  # 한글만 남기고 모두 제거
        tokens = okt.morphs(sentence.strip(), stem=True)

        # 토큰의 길이가 1 이상이고 frequent_words.txt에 없는 경우에만 리스트에 추가
        tokens = [token for token in tokens if len(token) > 1 and token not in frequent_words]
        if tokens:
            main_tokens.append(tokens)

    # 빈 리스트가 아닌 경우에만 처리된 데이터를 새로운 딕셔너리에 저장
    if main_tokens:
        processed_data.append({
            "title_tokens": title_tokens,
            "main_tokens": main_tokens
        })

# 새로운 JSON 파일로 저장
with open(f"processed_data_{today_date}_100.json", 'w', encoding='utf-8') as output_file:
    json.dump(processed_data, output_file, ensure_ascii=False, indent=4)

# 결과 DataFrame 생성
processed_data_df = spark.createDataFrame(processed_data)

# 결과를 HDFS에 저장
output_path = f"hdfs://<HDFS_MASTER_IP>:<HDFS_PORT>/path/to/your/output/folder"
processed_data_df.write.mode("overwrite").json(output_path)

# Spark 종료
spark.stop()
