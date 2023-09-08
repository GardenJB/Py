from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col
from konlpy.tag import Okt
import re
from datetime import datetime
import json

# 현재 날짜를 얻어 파일 이름 생성
today_date = datetime.now().strftime('%Y-%m-%d')
sids = [i for i in range(100, 106)]
for sid in sids:
  # Spark 설정 및 SparkSession 생성
  conf = SparkConf().setAppName("Preprocessing")
  sc = SparkContext(conf=conf)
  spark = SparkSession(sc)

  # HDFS 경로
  # data_path = "hdfs://<HDFS_MASTER_IP>:<HDFS_PORT>/path/to/your/file.json"
  data_path = "http://localhost:9000/article/article_data_{today_date}_{sid}.json"
  # output_path = "hdfs://<HDFS_MASTER_IP>:<HDFS_PORT>/path/to/your/output/folder"
  output_path = "http://localhost:9000/article/preprocessed_data_{today_date}_{sid}.json"

  # 로컬 파일 로드
  with open("frequent_words.txt", 'r', encoding='utf-8') as freq_file:
    frequent_words = set(freq_file.read().splitlines())

  # HDFS에서 데이터 파일을 읽기
  art_df = spark.read.json(data_path)

  # Okt 초기화
  okt = Okt()

  # 정규표현식 패턴: 한글만 남기고 모두 제거 함수
  hangul_pattern = re.compile('[^ㄱ-ㅎㅏ-ㅣ가-힣]+')

  # DataFrame을 RDD로 변환
  art_rdd = art_df.rdd

  # 새로운 JSON 데이터를 담을 리스트 초기화
  processed_data = art_rdd.flatMap(lambda data: process_data(data, frequent_words, okt, hangul_pattern))

  # 새로운 JSON 파일로 저장
  processed_data.saveAsTextFile(output_path)

  # Spark 종료
  spark.stop()


  def process_data(data, frequent_words, okt, hangul_pattern):
    title_text = data.get("title", "")
    main_text = data.get("main", "")

    # 텍스트 데이터를 Okt를 사용하여 처리
    title_text = hangul_pattern.sub('', title_text)
    title_tokens = okt.morphs(title_text, stem=True)

    # "title_tokens"에 대해서 토큰의 길이가 1 이상이고 frequent_words.txt에 없는 경우에만 리스트에 추가
    title_tokens = [token for token in title_tokens if len(token) > 1 and token not in frequent_words]

    # main_text를 문장 단위로 분할하여 리스트로
    main_sentences = main_text.split('.')

    # 각 문장을 Okt를 사용하여 처리하고 리스트에 추가
    main_tokens = []
    for sentence in main_sentences:
      sentence = hangul_pattern.sub('', sentence)
      tokens = okt.morphs(sentence.strip(), stem=True)

      # 토큰의 길이가 1 이상이고 frequent_words.txt에 없는 경우에만 리스트에 추가
      tokens = [token for token in tokens if len(token) > 1 and token not in frequent_words]
      if tokens:
        main_tokens.append(tokens)

    # 빈 리스트가 아닌 경우에만 처리된 데이터를 반환
    if main_tokens:
      return [{
        "title_tokens": title_tokens,
        "main_tokens": main_tokens
      }]
    else:
      return []
