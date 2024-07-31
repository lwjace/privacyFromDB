from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import random
import string

# Cassandra 클러스터에 연결
def get_cassandra_session():
    # Ubuntu에서 실행 중인 Cassandra에 접속하기 위한 IP 주소와 포트 번호를 설정합니다.
    cluster = Cluster(['127.0.0.1'], port=9042)
    # 인증이 필요한 경우 아래와 같이 설정합니다.
    # auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    # cluster = Cluster(['127.0.0.1'], port=9042, auth_provider=auth_provider)
    session = cluster.connect()
    return session

# 임의의 테이블 이름 생성
def random_table_name():
    return 'table_' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))

# 테이블 생성 및 데이터 삽입
def create_tables_and_insert_data(session):
    keyspace = 'test_keyspace'
    session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}")
    session.set_keyspace(keyspace)

    for _ in range(10):
        table_name = random_table_name()
        session.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id UUID PRIMARY KEY,
            name text,
            value int
        )
        """)

        for _ in range(30):
            session.execute(f"""
            INSERT INTO {table_name} (id, name, value) VALUES (uuid(), %s, %s)
            """, (random.choice(['Alice', 'Bob', 'Charlie']), random.randint(1, 100)))

if __name__ == "__main__":
    session = get_cassandra_session()
    create_tables_and_insert_data(session)
    print("테이블 생성 및 데이터 삽입 완료")
    session.shutdown()
