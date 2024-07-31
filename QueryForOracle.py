import cx_Oracle

# 데이터베이스 연결 설정
def get_connection():
    dsn = cx_Oracle.makedsn("db.example.com", 1521, service_name="ORCL")
    connection = cx_Oracle.connect(user="admin", password="your_password_here", dsn=dsn)
    return connection

def main():
    connection = get_connection()
    cursor = connection.cursor()

    # 예시 쿼리 실행
    cursor.execute("SELECT table_name FROM user_tables")
    for table_name in cursor:
        print(table_name)

    cursor.close()
    connection.close()

if __name__ == "__main__":
    main()
