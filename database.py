import pymysql

def connect_mysql():
    return pymysql.connect(
        host="localhost",
        port=3306,
        user="root",
        password="admin123",
        database="document_db"
    )
