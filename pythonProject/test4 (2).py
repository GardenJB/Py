import json
from konlpy.tag import Okt
from tqdm import tqdm
from datetime import datetime
import re

# 전처리 시작
# 파일 불러오기
today_date = datetime.now().strftime('%Y-%m-%d')

with open(f"article_data_{today_date}_100.json", 'r', encoding='utf-8') as file:
  art_df = json.load(file)

# frequent_words
# 텍스트 파일을 읽어와서 각 단어를 목록으로 만드는 함수
def read_text_file_to_list(file_path):
    word_list = []  # 각 단어를 저장할 빈 목록 생성

    # 텍스트 파일 열기
    with open(file_path, 'r', encoding='utf-8') as file:
        # 파일 내용을 줄 단위로 읽어오기
        lines = file.readlines()

        # 각 줄에서 단어 추출하여 목록에 추가
        for line in lines:
            # 줄 바꿈 문자로 분할하여 각 단어 추출
            words = line.strip().split('\n')

            # 추출된 단어를 목록에 추가
            word_list.extend(words)

    return word_list

# 텍스트 파일 경로 설정 (현재 폴더 내에 있는 'text_file.txt' 파일로 가정)
file_path = 'frequent_words.txt'

# 텍스트 파일을 읽어와서 단어 목록 생성
word_list = read_text_file_to_list(file_path)

# # 결과 확인
# print(word_list)


# Okt 초기화
okt = Okt()

# 새로운 JSON 데이터를 담을 리스트 초기화
processed_data = []

# 각 원소에 대해 "title"과 "main" 키를 찾아서 처리
for data in tqdm(art_df):
  title_text = data.get("title", "")
  main_text = data.get("main", "")

  # 텍스트 데이터를 Okt를 사용하여 처리
  title_tokens = okt.morphs(title_text)
  main_tokens = okt.morphs(main_text)
  # title_tokens = okt.pos(title_text)
  # main_tokens = okt.pos(main_text)

  # 처리된 데이터 중에서 제거하고 싶은 토큰 제외하고 저장
  title_tokens = [token for token in title_tokens if
                  (token[0] not in word_list) ]
  main_tokens = [token for token in main_tokens if
                 token[0] not in word_list  ]

  # 처리된 데이터를 새로운 딕셔너리에 저장
  processed_data.append({
    "title_tokens": title_tokens,
    "main_tokens": main_tokens
  })

  print(processed_data)

# # 새로운 JSON 파일로 저장
# with open(f"processed_data_filter_{today_date}_100.json", 'w', encoding='utf-8') as output_file:
#   json.dump(processed_data, output_file, ensure_ascii=False, indent=4)
#
# with open(f"processed_data_filter_{today_date}_100.json", 'r', encoding='utf-8') as file:
#     data = json.load(file)
#     print(data)