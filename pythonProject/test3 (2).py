import json
from konlpy.tag import Okt
from tqdm import tqdm
from datetime import datetime
from collections import Counter

# 전처리 시작
# 파일 불러오기
today_date = datetime.now().strftime('%Y-%m-%d')

# with open(f"article_data_{today_date}_100.json", 'r', encoding='utf-8') as file:
with open(f"article_data_100p_100.json", 'r', encoding='utf-8') as file:
  art_df = json.load(file)

# Okt 초기화
okt = Okt()

# 새로운 JSON 데이터를 담을 리스트 초기화
processed_data = []

for data in tqdm(art_df):
  title_text = data.get("title", "")
  main_text = data.get("main", "")


  # 텍스트 데이터를 Okt를 사용하여 처리
  # title_tokens = okt.morphs(title_text)
  # main_tokens = okt.morphs(main_text)
  # title_tokens = okt.pos(title_text)
  # main_tokens = okt.pos(main_text)

  # 명사만
  title_noun = okt.nouns(title_text)
  main_noun = okt.nouns(main_text)


  # 한글자 제거
  for i,v in enumerate(title_noun):
    if len(v)<2:
      title_noun.pop(i)

  for i, v in enumerate(main_noun):
    if len(v) < 2:
      main_noun.pop(i)

  title_count = Counter(title_noun)
  main_count = Counter(main_noun)

  title_noun_list = title_count.most_common(100)
  main_noun_list = main_count.most_common(100)



  # 처리된 데이터를 새로운 딕셔너리에 저장
  processed_data.append({
    "title_noun_100": title_noun_list,
    "main_noun_100": main_noun_list
  })

# 새로운 JSON 파일로 저장
# with open(f"processed_data_noun_{today_date}_100.json", 'w', encoding='utf-8') as output_file:
with open(f"processed_data_noun_100p_100.json", 'w', encoding='utf-8') as output_file:
  json.dump(processed_data, output_file, ensure_ascii=False, indent=4)

# with open(f"processed_data_noun_{today_date}_100.json", 'r', encoding='utf-8') as file:
#   data = json.load(file)
#   print(data)
