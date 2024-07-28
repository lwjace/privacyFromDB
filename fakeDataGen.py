from faker import Faker
import pandas as pd
import os

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

# 저장할 디렉토리 설정
save_dir = './data'
os.makedirs(save_dir, exist_ok=True)

# 파일 경로 설정
file_path = os.path.join(save_dir, 'fake_personal_data_extended_ko.csv')

# 데이터프레임을 CSV 파일로 저장
df.to_csv(file_path, index=False, encoding='utf-8-sig')

# 데이터프레임 출력
print(f"CSV 파일이 다음 경로에 저장되었습니다: {file_path}")
