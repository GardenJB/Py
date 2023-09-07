import re
from konlpy.tag import Okt
from tqdm import tqdm
from datetime import datetime
import json
import time

# 코드 시작 시간 기록
start_time = time.time()

# 전처리 시작
# 파일 불러오기
today_date = datetime.now().strftime('%Y-%m-%d')

# frequent_words.txt 파일을 불러와서 집합(set)으로 저장
with open("frequent_words.txt", 'r', encoding='utf-8') as freq_file:
    frequent_words = set(freq_file.read().splitlines())

# with open(f"article_data_{today_date}_100.json", 'r', encoding='utf-8') as file:
with open(f"article_data_100p_100.json", 'r', encoding='utf-8') as file:
    art_df = json.load(file)

# Okt 초기화
okt = Okt()

# 정규표현식 패턴: 한글만 남기고 모두 제거 함수
hangul_pattern = re.compile('[^ㄱ-ㅎㅏ-ㅣ가-힣]+')

# 새로운 JSON 데이터를 담을 리스트 초기화
processed_data = []

# 각 원소에 대해 "title"과 "main" 키를 찾아서 처리
for data in tqdm(art_df):
    title_text = data.get("title", "")
    main_text = data.get("main", "")

    # 텍스트 데이터를 Okt를 사용하여 처리
    title_text = hangul_pattern.sub('', title_text)  # 한글만 남기고 모두 제거
    title_tokens = okt.morphs(title_text, stem=True)

    # main_text를 문장 단위로 분할하여 리스트로 만듭니다.
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

    # "title_tokens"에 대해서도 토큰의 길이가 1 이상이고 frequent_words.txt에 없는 경우에만 리스트에 추가
    title_tokens = [token for token in title_tokens if len(token) > 1 and token not in frequent_words]

    # 빈 리스트가 아닌 경우에만 처리된 데이터를 새로운 딕셔너리에 저장
    if main_tokens:
        processed_data.append({
            "title_tokens": title_tokens,
            "main_tokens": main_tokens
        })

# 새로운 JSON 파일로 저장
with open(f"processed_data_{today_date}_100.json", 'w', encoding='utf-8') as output_file:
    json.dump(processed_data, output_file, ensure_ascii=False, indent=4)

# 코드 종료 시간 기록
end_time = time.time()

# 실행 시간 계산 및 출력
execution_time = end_time - start_time
print(f"코드 실행 시간: {execution_time:.2f} 초")
