# from pyspark import SparkConf, SparkContext
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import split, explode, col
from konlpy.tag import Okt
import re
from datetime import datetime
from tqdm import tqdm
import json

# # Spark 초기화
# conf = SparkConf().setAppName("PreProcessing")
# sc = SparkContext(conf=conf)
# spark = SparkSession(sc)


# 데이터 처리 함수 정의
def process_data(art_df, filter_words, okt, hangul_pattern):
  # 처리된 데이터를 저장할 리스트 초기화
  processed_data = []

  for data in tqdm(art_df):
    # 데이터에서 "title"과 "main"을 가져옴
    title_text = data.get("title", "")
    main_text = data.get("main", "")

    # 텍스트 데이터를 한글만 남기고 Okt를 사용하여 처리
    title_text = hangul_pattern.sub('', title_text)
    # 형태소
    title_tokens = okt.morphs(title_text, stem=True)

    # 토큰의 길이가 2 이상이고 불용어 제거한 결과를 담을 리스트
    filter_title_tokens = []
    for title_token in title_tokens:
      # 토큰의 길이가 2 이상이고 불용어 제거
      if len(title_token) > 1 and title_token not in filter_words:
        filter_title_tokens.append(title_token)

    # main_text를 문장으로 분할
    main_sentences = main_text.split('.')

    for sentence in main_sentences:
      sentence = hangul_pattern.sub('', sentence)
      # 본문 형태소 문장
      tokens = okt.morphs(sentence.strip(), stem=True)

      # 토큰의 길이가 2 이상이고 불용어 제거한 결과를 담을 리스트
      filter_tokens = []
      # 토큰의 길이가 2 이상이고 불용어 제거
      for token in tokens:
        if len(token) > 1 and token not in filter_words:
          filter_tokens.append(token)

      # 빈 리스트가 아닌 경우에만 처리된 데이터를 반환
      if filter_tokens:
        processed_data.append(filter_tokens)

    # 제목과 본문에서 추출된 토큰을 합쳐 리스트에 저장
    if filter_title_tokens:
      processed_data.append(filter_title_tokens)

  return processed_data


# 명사 데이터 처리 함수 정의
def process_noun_data(art_df, filter_words, okt, hangul_pattern):
  # 처리된 명사 데이터를 저장할 리스트 초기화
  processed_noun_data = []

  for data in tqdm(art_df):

    # 데이터에서 "title"과 "main"을 가져옴
    title_text = data.get("title", "")
    main_text = data.get("main", "")

    # 제목 텍스트에서 한글 이외의 문자 제거
    title_text = hangul_pattern.sub('', title_text)

    # 제목 텍스트에서 명사 추출
    title_nouns = okt.nouns(title_text)
    noun_tokens = []
    for noun_token in title_nouns:
      if len(noun_token) > 1 and noun_token not in filter_words:
        noun_tokens.append(noun_token)

    # 본문 텍스트를 문장으로 분할
    main_sentences = main_text.split('.')

    # 본문 텍스트 처리 및 명사 추출
    for sentence in main_sentences:
      sentence = hangul_pattern.sub('', sentence)
      nouns = okt.nouns(sentence.strip())
      for noun in nouns:
        if len(noun) > 1 and noun not in filter_words:
          noun_tokens.append(noun)

    if noun_tokens:
      processed_noun_data.append(noun_tokens)

  return processed_noun_data


# 현재 날짜를 파일 이름에 사용
today_date = datetime.now().strftime('%Y-%m-%d')

# sids 리스트 정의
sids = [i for i in range(100, 106)]

# 로컬 파일에서 필터 단어 읽기
with open("filter_words.txt", 'r', encoding='utf-8') as freq_file:
  filter_words = set(freq_file.read().splitlines())

for sid in sids:
  # # HDFS 경로 정의
  # data_path = f"http://localhost:9000/article/article_data_{today_date}_{sid}.json"
  # output_path = f"http://localhost:9000/article/preprocessed_data_{today_date}_{sid}.json"
  # output_noun_path = f"http://localhost:9000/article/preprocessed_noun_data_{today_date}_{sid}.json"

  with open(f"article_data_100p_{today_date}_{sid}.json", 'r', encoding='utf-8') as file:
    art_df = json.load(file)

  # # HDFS에서 데이터 읽기
  # art_df = spark.read.json(data_path)

  # Okt 초기화
  okt = Okt()

  # 정규표현식 패턴: 한글만 남기고 모두 제거
  hangul_pattern = re.compile('[^ㄱ-ㅎㅏ-ㅣ가-힣]+')

  # # DataFrame을 RDD로 변환
  # art_rdd = art_df.rdd

  processed_data = process_data(art_df, filter_words, okt, hangul_pattern)
  processed_noun_data = process_noun_data(art_df, filter_words, okt, hangul_pattern)

  # 새로운 JSON 파일로 저장
  with open(f"processed_data_{today_date}_{sid}.json", 'w', encoding='utf-8') as output_file:
    json.dump(processed_data, output_file, ensure_ascii=False, indent=4)

  # 명사 데이터를 저장
  with open(f"processed_noun_data_{today_date}_{sid}.json", 'w', encoding='utf-8') as output_file:
    json.dump(processed_noun_data, output_file, ensure_ascii=False, indent=4)

  # # 데이터 처리 및 텍스트 파일로 저장
  # processed_data = art_rdd.flatMap(lambda data: process_data(data, filter_words, okt, hangul_pattern))
  # processed_noun_data = art_rdd.flatMap(lambda data: process_noun_data(data, filter_words, okt, hangul_pattern))
#
#   processed_data.saveAsTextFile(output_path)
#   processed_noun_data.saveAsTextFile(output_noun_path)
#
# # Spark 종료
# spark.stop()
