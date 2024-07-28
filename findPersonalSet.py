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
    '전화번호': re.compile(r'(01[016789]-\d{3,4}-\d{4})|(0\d{1,2}-\d{3,4}-\d{4})'),
    '이메일': re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}'),
    '생년월일': re.compile(r'\d{4}-\d{2}-\d{2}'),
    '주민등록번호': re.compile(r'\d{6}-\d{7}')
}

# 마스킹 처리 함수
def mask_value(field, value):
    if field == '전화번호':
        return value[:-4] + '****'
    elif field == '이메일':
        username, domain = value.split('@')
        return '****' + '@' + domain
    elif field == '생년월일':
        value_str = str(value)
        return value_str[:5] + '**-**'
    elif field == '주민등록번호':
        return value[:-6] + '******'
    return value

# 테이블 이름 가져오기
get_tables_query = "SHOW TABLES;"

try:
    with connection.cursor() as cursor:
        # 모든 테이블 이름 가져오기
        cursor.execute(get_tables_query)
        tables = cursor.fetchall()

        # 필요한 테이블 이름 필터링 (find_data 테이블 제외)
        target_tables = [table[0] for table in tables if table[0] != 'find_data']

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
        
        # 새로운 테이블 생성
        cursor.execute(create_new_table_query)
        connection.commit()  # 테이블 생성 후 커밋

        for table in target_tables:
            for field, pattern in patterns.items():
                # 각 테이블의 컬럼 이름 가져오기
                cursor.execute(f"DESCRIBE {table}")
                columns = cursor.fetchall()

                for column in columns:
                    column_name = column[0]
                    
                    # 각 컬럼의 데이터를 정규 표현식으로 검사, 최대 30개 가져오기
                    cursor.execute(f"SELECT {column_name} FROM {table}")
                    rows = cursor.fetchall()
                    
                    matched_values = []
                    for row in rows:
                        value = row[0]
                        if value is None:
                            continue
                        
                        # 정규 표현식으로 값 검사
                        if pattern.match(str(value)):
                            masked_value = mask_value(field, value)
                            matched_values.append((field, table, column_name, masked_value))
                            if len(matched_values) >= 30:  # 매치된 데이터가 30개를 넘으면 종료
                                break
                    # 매치된 데이터를 새로운 테이블에 삽입
                    for matched_value in matched_values:
                        insert_query = """
                        INSERT INTO find_data (personal_data_type, table_name, column_name, matched_value)
                        VALUES (%s, %s, %s, %s)
                        """
                        cursor.execute(insert_query, matched_value)
        
        # 변경사항 커밋
        connection.commit()

finally:
    connection.close()

print("데이터 검출 및 삽입이 완료되었습니다.")
