import pyodbc
import random
import string

# MS SQL 서버에 접속하기 위한 정보 설정
server = 'DESKTOP-A952EQF,1433'  # 서버 이름과 포트 번호
database = 'engine'  # 데이터베이스 이름
username = 'user1'  # SQL Server 인증 사용자 이름
password = '1234abcd'  # SQL Server 인증 비밀번호

# 데이터베이스에 연결
try:
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=' + server + ';'
        'DATABASE=' + database + ';'
        'UID=' + username + ';'
        'PWD=' + password
    )

    # 커서 객체 생성
    cursor = conn.cursor()

    # 테이블 생성 및 데이터 삽입 함수 정의
    def create_and_insert_table(table_name):
        # 테이블이 존재하는지 확인하고, 존재하면 삭제
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
        conn.commit()

        # 테이블 생성 쿼리
        create_table_query = f'''
        CREATE TABLE {table_name} (
            id INT PRIMARY KEY IDENTITY(1,1),
            name NVARCHAR(50),
            value INT
        )
        '''
        cursor.execute(create_table_query)
        conn.commit()
        
        # 데이터 삽입 쿼리
        insert_query = f'''
        INSERT INTO {table_name} (name, value)
        VALUES (?, ?)
        '''
        
        # 임의의 데이터 생성 및 삽입
        for _ in range(30):
            name = ''.join(random.choices(string.ascii_letters, k=10))
            value = random.randint(1, 100)
            cursor.execute(insert_query, (name, value))
        
        conn.commit()

    # 10개의 테이블 생성 및 데이터 삽입
    for i in range(10):
        table_name = f'table_{i+1}'
        create_and_insert_table(table_name)

    # 각 테이블에 대한 데이터 확인
    for i in range(10):
        table_name = f'table_{i+1}'
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"Table {table_name} has {count} rows.")

    # 연결 종료
    cursor.close()
    conn.close()

except pyodbc.Error as e:
    print("Error in connection:", e)
