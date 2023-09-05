import os
import multiprocessing
import json
from konlpy.tag import Okt
from tqdm import tqdm
from datetime import datetime, timedelta

# 파일을 처리하는 함수
def process_file(file_path):
  with open(file_path, 'r', encoding='utf-8') as file:
    art_df = json.load(file)
  # Okt 초기화
  okt = Okt()

  # 새로운 JSON 데이터를 담을 리스트 초기화
  processed_data = []

  # 각 원소에 대해 "title"과 "main" 키를 찾아서 처리
  for data in tqdm(art_df):
    title_text = data.get("title", "")
    main_text = data.get("main", "")

    # 텍스트 데이터를 Okt를 사용하여 처리
    title_tokens = okt.morphs(title_text, stem=True)
    main_tokens = okt.morphs(main_text, stem=True)

    # title_tokens = okt.pos(title_text)
    # main_tokens = okt.pos(main_text)

    # 처리된 데이터를 새로운 딕셔너리에 저장
    processed_data.append({
      "title_tokens": title_tokens,
      "main_tokens": main_tokens
    })

  # 새로운 JSON 파일로 저장
  with open(f"processed_data_{today_date}_100.json", 'w', encoding='utf-8') as output_file:
    # with open(f"processed_data_100p_100.json", 'w', encoding='utf-8') as output_file:
    json.dump(processed_data, output_file, ensure_ascii=False, indent=4)

  # with open(f"processed_data_{today_date}_101.json", 'r', encoding='utf-8') as file:
  #     data = json.load(file)
  #     print(data)


if __name__ == "__main__":
  today_date = datetime.now()
  sids = [i for i in range(100, 106)]
  for sid in sids:
    # 병렬 처리할 파일 목록
    # file_list = [f"article_data_100p_{sid}.json"]
    file_list = [f"article_data_100p_{sid}.json", f"article_data_100p_2023-09-04_{sid}.json", f"article_data_100p_2023-09-05_{sid}.json"]
    # for i in range(7):
    #   date = today_date - timedelta(days=i)
    #   date_str = date.strftime('%Y-%m-%d')
    #   file_name = f"article_data_100p_{date_str}_{sid}.json"
    #   file_list.append(file_name)

    # # 파일 목록을 순회하면서 각 파일을 처리
    # for file_path in file_list:
    #   process_file(file_path)

    # CPU 코어 수 만큼 프로세스를 생성하여 병렬로 처리
    num_cores = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=num_cores)

    # 파일 목록을 각 프로세스에 분배하여 병렬로 처리
    pool.map(process_file, file_list)

    # 작업이 완료될 때까지 대기
    pool.close()
    pool.join()

