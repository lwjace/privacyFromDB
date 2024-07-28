from faker import Faker
import pandas as pd
import os
import pymysql

# Faker 객체 생성, 로케일을 한국어로 설정
fake = Faker('ko_KR')

# 생성할 데이터 수
num_samples = 100

# 가짜 데이터 저장할 리스트
data = []

# 가짜 데이터 생성
for _ in range(num_samples):
    data.append({
        '이름': fake.name(),
        '주소': fake.address(),
        '전화번호': fake.phone_number(),
        '이메일': fake.email(),
        '생년월일': fake.date_of_birth(),
        '주민등록번호': fake.ssn(),
        '회사명': fake.company(),
        '직업': fake.job(),
        '사용자이름': fake.user_name(),
        '비밀번호': fake.password(),
        '웹사이트': fake.url(),
        '신용카드번호': fake.credit_card_number(),
        '신용카드만료일': fake.credit_card_expire(),
        '신용카드발급사': fake.credit_card_provider(),
        'IBAN': fake.iban(),
        'SWIFT코드': fake.swift(),
        '차량번호판': fake.license_plate(),
        'MAC주소': fake.mac_address(),
        'IPv4주소': fake.ipv4(),
        'IPv6주소': fake.ipv6(),
        'UUID': fake.uuid4()
    })

# 데이터프레임으로 변환
df = pd.DataFrame(data)

# 데이터베이스 연결 설정
connection = pymysql.connect(
    host='127.0.0.1',  # 데이터베이스 호스트 이름 (예: 'localhost' 또는 실제 IP 주소)
    user='root',  # 데이터베이스 사용자 이름
    password='1234abcd',  # 데이터베이스 비밀번호
    database='cust',  # 데이터베이스 이름
    charset='utf8mb4'
)

# 테이블 생성 (만약 존재하지 않는다면)
create_table_query = """
CREATE TABLE IF NOT EXISTS fake_cust_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    이름 VARCHAR(50),
    주소 VARCHAR(255),
    전화번호 VARCHAR(20),
    이메일 VARCHAR(100),
    생년월일 DATE,
    주민등록번호 VARCHAR(14),
    회사명 VARCHAR(100),
    직업 VARCHAR(100),
    사용자이름 VARCHAR(50),
    비밀번호 VARCHAR(100),
    웹사이트 VARCHAR(100),
    신용카드번호 VARCHAR(20),
    신용카드만료일 VARCHAR(5),
    신용카드발급사 VARCHAR(50),
    IBAN VARCHAR(34),
    SWIFT코드 VARCHAR(11),
    차량번호판 VARCHAR(20),
    MAC주소 VARCHAR(17),
    IPv4주소 VARCHAR(15),
    IPv6주소 VARCHAR(39),
    UUID VARCHAR(36)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
"""

# 데이터 삽입
insert_query = """
INSERT INTO fake_cust_data (
    이름, 주소, 전화번호, 이메일, 생년월일, 주민등록번호, 회사명, 직업, 사용자이름, 비밀번호, 웹사이트, 신용카드번호, 신용카드만료일, 신용카드발급사, IBAN, SWIFT코드, 차량번호판, MAC주소, IPv4주소, IPv6주소, UUID
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
);
"""

try:
    with connection.cursor() as cursor:
        # 테이블 생성
        cursor.execute(create_table_query)
        
        # 데이터 삽입
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                row['이름'], row['주소'], row['전화번호'], row['이메일'], row['생년월일'], row['주민등록번호'], row['회사명'],
                row['직업'], row['사용자이름'], row['비밀번호'], row['웹사이트'], row['신용카드번호'], row['신용카드만료일'],
                row['신용카드발급사'], row['IBAN'], row['SWIFT코드'], row['차량번호판'], row['MAC주소'], row['IPv4주소'],
                row['IPv6주소'], row['UUID']
            ))
    
    # 커밋
    connection.commit()

finally:
    connection.close()

print("데이터가 성공적으로 삽입되었습니다.")
