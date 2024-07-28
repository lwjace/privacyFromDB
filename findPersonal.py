import pymysql
import re

# 데이터베이스 연결 설정
connection = pymysql.connect(
    host='127.0.0.1',
    user='root',
    password='1234abcd',
    database='cust',
    charset='utf8mb4'
)

# 정규 표현식 패턴 설정
patterns = {
    '이름': re.compile(r'[가-힣]{2,3}'),
    '주소': re.compile(r'[가-힣0-9\s\-]+'),
    '전화번호': re.compile(r'(01[016789]-\d{3,4}-\d{4})|(0\d{1,2}-\d{3,4}-\d{4})'),
    '이메일': re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}'),
    '생년월일': re.compile(r'\d{4}-\d{2}-\d{2}'),
    '주민등록번호': re.compile(r'\d{6}-\d{7}')
}

# 새로운 테이블 생성 쿼리
create_new_table_query = """
CREATE TABLE IF NOT EXISTS find_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    personal_data_type VARCHAR(255),
    table_name VARCHAR(255),
    column_name VARCHAR(255),
    matched_value TEXT
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
"""

# 테이블 이름 가져오기
get_tables_query = "SHOW TABLES;"

try:
    with connection.cursor() as cursor:
        # 새로운 테이블 생성
        cursor.execute(create_new_table_query)
        connection.commit()  # 테이블 생성 후 커밋

        # 테이블 구조 확인
        cursor.execute("DESCRIBE find_data;")
        find_data_columns = cursor.fetchall()
        print("find_data 테이블 구조:", find_data_columns)

        # 모든 테이블 이름 가져오기
        cursor.execute(get_tables_query)
        tables = cursor.fetchall()

        matched_count = 0  # 매치된 데이터의 개수를 카운트하기 위한 변수

        for (table,) in tables:
            # 각 테이블의 컬럼 이름 가져오기
            cursor.execute(f"DESCRIBE {table}")
            columns = cursor.fetchall()

            for column in columns:
                column_name = column[0]
                
                # 각 컬럼의 데이터를 정규 표현식으로 검사
                cursor.execute(f"SELECT {column_name} FROM {table}")
                rows = cursor.fetchall()
                
                for row in rows:
                    value = row[0]
                    if value is None:
                        continue
                    
                    # 정규 표현식으로 값 검사
                    for field, pattern in patterns.items():
                        if pattern.match(str(value)):
                            # 일치하는 데이터 새로운 테이블에 삽입
                            insert_query = """
                            INSERT INTO find_data (personal_data_type, table_name, column_name, matched_value)
                            VALUES (%s, %s, %s, %s)
                            """
                            cursor.execute(insert_query, (field, table, column_name, value))
                            matched_count += 1
                            if matched_count >= 30:  # 매치된 데이터가 30개를 넘으면 종료
                                break
                    if matched_count >= 30:
                        break
                if matched_count >= 30:
                    break
            if matched_count >= 30:
                break
        
        # 변경사항 커밋
        connection.commit()

finally:
    connection.close()

print("데이터 검출 및 삽입이 완료되었습니다.")
