from pymongo import MongoClient
import random
import string

# MongoDB에 연결
client = MongoClient('mongodb://localhost:27017/')

# 사용할 데이터베이스 선택
db = client['engine']

# 랜덤 문자열 생성 함수
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# 10개의 컬렉션 생성 및 각 컬렉션에 30개의 문서 삽입
for i in range(10):
    collection_name = f'collection_{i+1}'
    collection = db[collection_name]
    
    documents = []
    for _ in range(30):
        document = {
            'name': random_string(),
            'value': random.randint(1, 100),
            'description': random_string(20)
        }
        documents.append(document)
    
    collection.insert_many(documents)

print("컬렉션 및 문서 생성 완료.")
