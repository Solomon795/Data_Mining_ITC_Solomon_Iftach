import pymysql
connection = pymysql.connect(user='root', password='root', database='datamining')
cursor = connection.cursor()
cursor.execute("SHOW TABLES")
print(cursor.fetchall())
cursor.close()
connection.close()

