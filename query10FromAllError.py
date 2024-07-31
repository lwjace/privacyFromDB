import os
import re
import pandas as pd
from sqlalchemy import create_engine
from pymongo import MongoClient
from cassandra.cluster import Cluster
import pyodbc
import traceback

def read_config(file_path):
    config = {}
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            line = line.strip()
            if line and not line.startswith('#'):  # 빈 줄이나 주석 무시
                key, value = line.split('=')
                config[key.strip()] = value.strip()
    return config

def sanitize_sheet_name(sheet_name):
    # 시트 이름에서 사용할 수 없는 문자 제거
    return re.sub(r'[\/\\\*\?\:\[\]]', '_', sheet_name)

def log_error_to_excel(db_info, error_message, file_name='error_log.xlsx'):
    if not os.path.exists(file_name):
        # Create a new Excel file with an initial sheet
        with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
            pd.DataFrame().to_excel(writer, sheet_name='init')

    error_data = pd.DataFrame({'Database Info': [db_info], 'Error Message': [error_message]})
    sanitized_sheet_name = sanitize_sheet_name(db_info[:31])
    with pd.ExcelWriter(file_name, engine='openpyxl', mode='a') as writer:
        error_data.to_excel(writer, sheet_name=sanitized_sheet_name)

def fetch_cassandra_data(host, keyspace, error_file):
    try:
        cluster = Cluster([host])
        session = cluster.connect(keyspace)
        tables = session.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{keyspace}'")
        dataframes = {}
        for table in tables:
            query = f"SELECT * FROM {table.table_name} LIMIT 10"
            rows = session.execute(query)
            df = pd.DataFrame(rows)
            if not df.empty:
                dataframes[f"{keyspace}.{table.table_name}"] = df
        if not dataframes:
            log_error_to_excel(f"Cassandra Host: {host}, Keyspace: {keyspace}", "No data fetched", error_file)
        return dataframes
    except Exception as e:
        log_error_to_excel(f"Cassandra Host: {host}, Keyspace: {keyspace}", str(e), error_file)
        return {}

def fetch_mongodb_data(uri, database, error_file):
    try:
        client = MongoClient(uri)
        db = client[database]
        collections = db.list_collection_names()
        dataframes = {}
        for collection in collections:
            cursor = db[collection].find().sort('_id', -1).limit(10)
            df = pd.DataFrame(list(cursor))
            if not df.empty:
                dataframes[f"{database}.{collection}"] = df
        if not dataframes:
            log_error_to_excel(f"MongoDB URI: {uri}, Database: {database}", "No data fetched", error_file)
        return dataframes
    except Exception as e:
        log_error_to_excel(f"MongoDB URI: {uri}, Database: {database}", str(e), error_file)
        return {}

def fetch_mariadb_data(host, user, password, database, error_file):
    try:
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")
        tables = pd.read_sql("SHOW TABLES", engine)
        dataframes = {}
        for table in tables.values.flatten():
            query = f"SELECT * FROM {table} ORDER BY id DESC LIMIT 10"
            df = pd.read_sql(query, engine)
            if not df.empty:
                dataframes[f"{database}.{table}"] = df
        if not dataframes:
            log_error_to_excel(f"MariaDB Host: {host}, User: {user}, Database: {database}", "No data fetched", error_file)
        return dataframes
    except Exception as e:
        log_error_to_excel(f"MariaDB Host: {host}, User: {user}, Database: {database}", str(e), error_file)
        return {}

def fetch_mssql_data(host, user, password, database, error_file):
    try:
        connection_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={database};UID={user};PWD={password}"
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={connection_str}")
        tables = pd.read_sql("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'", engine)
        dataframes = {}
        for table in tables['TABLE_NAME']:
            try:
                query = f"SELECT TOP 10 * FROM {table} ORDER BY id DESC"
                df = pd.read_sql(query, engine)
                if not df.empty:
                    dataframes[f"{database}.{table}"] = df
            except Exception as table_exception:
                log_error_to_excel(f"MSSQL Host: {host}, User: {user}, Database: {database}, Table: {table}", str(table_exception), error_file)
        if not dataframes:
            log_error_to_excel(f"MSSQL Host: {host}, User: {user}, Database: {database}", "No data fetched", error_file)
        return dataframes
    except Exception as e:
        log_error_to_excel(f"MSSQL Host: {host}, User: {user}, Database: {database}", str(e), error_file)
        return {}

def save_to_excel(dataframes, file_name):
    if dataframes:
        with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
            for table_name, df in dataframes.items():
                df.to_excel(writer, sheet_name=table_name[:31])

def main():
    config = read_config('db_config.txt')
    error_file = 'error_log.xlsx'
    
    # Cassandra
    cassandra_hosts = [key for key in config.keys() if key.startswith('ca_host')]
    for host_key in cassandra_hosts:
        host_number = host_key.replace('ca_host', '')
        keyspace_key = f'ca_keyspace{host_number}'
        if keyspace_key in config:
            cassandra_data = fetch_cassandra_data(config[host_key], config[keyspace_key], error_file)
            if cassandra_data:
                save_to_excel(cassandra_data, f'cassandra_latest_rows_{host_number}.xlsx')
    
    # MongoDB
    mongodb_uris = [key for key in config.keys() if key.startswith('mo_uri')]
    for uri_key in mongodb_uris:
        uri_number = uri_key.replace('mo_uri', '')
        database_key = f'mo_database{uri_number}'
        if database_key in config:
            mongodb_data = fetch_mongodb_data(config[uri_key], config[database_key], error_file)
            if mongodb_data:
                save_to_excel(mongodb_data, f'mongodb_latest_rows_{uri_number}.xlsx')
    
    # MariaDB
    mariadb_hosts = [key for key in config.keys() if key.startswith('ma_host')]
    for host_key in mariadb_hosts:
        host_number = host_key.replace('ma_host', '')
        user_key = f'ma_user{host_number}'
        password_key = f'ma_password{host_number}'
        database_key = f'ma_database{host_number}'
        if user_key in config and password_key in config and database_key in config:
            mariadb_data = fetch_mariadb_data(config[host_key], config[user_key], config[password_key], config[database_key], error_file)
            if mariadb_data:
                save_to_excel(mariadb_data, f'mariadb_latest_rows_{host_number}.xlsx')
    
    # MSSQL
    mssql_hosts = [key for key in config.keys() if key.startswith('ms_host')]
    for host_key in mssql_hosts:
        host_number = host_key.replace('ms_host', '')
        user_key = f'ms_user{host_number}'
        password_key = f'ms_password{host_number}'
        database_key = f'ms_database{host_number}'
        if user_key in config and password_key in config and database_key in config:
            mssql_data = fetch_mssql_data(config[host_key], config[user_key], config[password_key], config[database_key], error_file)
            if mssql_data:
                save_to_excel(mssql_data, f'mssql_latest_rows_{host_number}.xlsx')

if __name__ == "__main__":
    main()
