from pyhive import hive
from sqlalchemy import create_engine
import pandas as pd

# Hive 연결 정보 설정
hive_host = 'your_hive_host'
hive_port = 10000
hive_database = 'your_database_name'
hive_user = 'your_username'
hive_password = 'your_password'

# Hive 연결 생성
hive_conn = hive.Connection(
    host=hive_host,
    port=hive_port,
    username=hive_user,
    password=hive_password,
    database=hive_database
)

# SQLAlchemy 엔진 생성
hive_engine = create_engine('hive://')

# Hive에 데이터 저장 예시
hive_query = "INSERT INTO your_hive_table (column1, column2) VALUES ('value1', 'value2')"
hive_engine.execute(hive_query)

# MySQL 연결 정보 설정
mysql_host = 'your_mysql_host'
mysql_port = 3306
mysql_database = 'your_mysql_database'
mysql_user = 'your_mysql_username'
mysql_password = 'your_mysql_password'

# SQLAlchemy 엔진 생성 (MySQL)
mysql_engine = create_engine(f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_database}")

# MySQL에 데이터 저장 예시
mysql_query = "INSERT INTO your_mysql_table (column1, column2) VALUES (%s, %s)"
data_to_insert = [('value1', 'value2'), ('value3', 'value4')]  # 데이터 예시

# 데이터프레임을 사용하여 MySQL에 데이터 저장
df = pd.DataFrame(data_to_insert, columns=['column1', 'column2'])
df.to_sql('your_mysql_table', mysql_engine, if_exists='append', index=False)

# 연결 닫기
hive_conn.close()
