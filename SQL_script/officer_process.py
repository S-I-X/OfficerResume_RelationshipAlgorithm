import psycopg2
import pymysql
from py2neo import Graph

option_postgre = {'database': "cof", 'user': "postgres", 'password': "postgres", 'host': "192.168.10.6"}  #PostgreSQL数据库连接验证

def Con_MySQL(database=None, user=None, password=None, host=None, port=3306):
    """
    :param database: 数据库名称
    :param user: 该数据库的使用者
    :param password: 密码
    :param host: 数据库地址
    :param port: 数据库链接的端口号
    :return: 返回数据库链接
    """
    connMy = pymysql.connect(db=database, user=user, passwd=password, host=host, port=port, charset="utf8")
    print("MySQL Connect successful!")
    return connMy

def Con_PostgreSQL(database=None, user=None, password=None, host=None, port=5432):
    """
    :param database: 数据库名称
    :param user: 该数据库的使用者
    :param password: 密码
    :param host: 数据库地址
    :param port: 数据库链接的端口号
    :return: 返回数据库链接
    """
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    print('PostgreSQL Connect successful!')
    return conn

def Con_Neo4j(http=None, user=None, password=None):
    """
    :param http: 数据库地址
    :param user: 该数据库的使用者
    :param password: 密码
    :return: 返回数据库链接
    """
    connection = Graph(http, username=user, password=password)  # 连接图数据库
    print('Neo4j Connect successful!')
    return connection

def init_countrymen_database(option_database=None):
    """
    该函数根据'officer_message'数据库初始化同乡数据库'officer_countrymen'，实现所有人的同乡关系匹配，
    忽略了籍贯为空的记录，双向关系只保存其中一个，标记'8'代表同乡关系
    :param option_database: 数据库验证配置信息
    :return: 无返回内容
    """
    Connection = Con_PostgreSQL(**option_database)
    select_sql = "SELECT id_index, officer_name, place_of_birth FROM crawler.officer_message WHERE place_of_birth != '';"
    insert_sql = "INSERT INTO crawler.officer_countrymen VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');"
    cur = Connection.cursor()
    cur.execute(select_sql)
    i = 0
    for x in cur.fetchall():
        cur.execute(
            f"SELECT id_index, officer_name, place_of_birth FROM crawler.officer_message WHERE place_of_birth = '{result[2]}' "
            f"AND place_of_birth  != '';")
        for y in cur.fetchall():
            if y[0] == x[0]:
                continue
            cur.execute(
                f"SELECT * FROM crawler.officer_countrymen WHERE (id_x = '{result[0]}' AND id_y = '{y[0]}') "
                f"OR (id_x = '{y[0]}' AND id_y = '{result[0]}');")
            if not cur.fetchone():
                finish_insert_sql = insert_sql.format(x[0], y[0], x[1], y[1], x[2], 8)
                cur.execute(finish_insert_sql)
                Connection.commit()
                i += 1
                print(i, finish_insert_sql)
    cur.close()
    Connection.close()
    print('同乡数据库初始化完成')

def init_schoolfellow_database(option_database=None):
    """
    该函数根据'officer_message'数据库初始化同乡数据库'officer_schoolfellow'，实现所有人的校友关系匹配;
    忽略了学校为空的记录，双向关系只保存其中一个，标记'7'代表校友关系
    :param option_database: 数据库验证配置信息
    :return: 无返回内容
    """
    Connection = Con_PostgreSQL(**option_database)
    select_sql = "SELECT id_index, officer_name, educational_university FROM crawler.officer_message WHERE educational_university != '';"
    insert_sql = "INSERT INTO crawler.officer_schoolfellow VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');"
    cur = Connection.cursor()
    cur.execute(select_sql)
    i = 0
    for x in cur.fetchall():
        cur.execute(
            f"SELECT id_index, officer_name, educational_university FROM crawler.officer_message WHERE educational_university = '{result[2]}' "
            f"AND educational_university  != '';")
        for y in cur.fetchall():
            if y[0] == x[0]:
                continue
            cur.execute(
                f"SELECT * FROM crawler.officer_schoolfellow WHERE (id_x = '{result[0]}' AND id_y = '{y[0]}') "
                f"OR (id_x = '{y[0]}' AND id_y = '{result[0]}');")
            if cur.fetchone() is None:
                finish_insert_sql = insert_sql.format(x[0], y[0], x[1], y[1], x[2], 7)
                cur.execute(finish_insert_sql)
                Connection.commit()
                i += 1
                print(i, finish_insert_sql)
    cur.close()
    Connection.close()
    print('校友数据库初始化完成')

def select_countrymen(id_selece=None, option_database=None):
    """
    :param id_selece: 需要查找同乡关系的人物ID
    :param option_database: 数据库验证配置信息
    :return: 该人物的所有同乡人物ID
    """
    Connection = Con_PostgreSQL(**option_database)
    select_sql = f"SELECT id_x, id_y FROM crawler.officer_countrymen WHERE id_x = '{id_selece}' OR id_y = '{id_selece}';"
    cur = Connection.cursor()
    cur.execute(select_sql)
    id_group = []
    for x in cur.fetchall():
        #print(result)
        if x[0] == id_selece and x[1] not in id_group:
            id_group.append(x[1])
        if x[1] == id_selece and x[0] not in id_group:
            id_group.append(x[0])
    cur.close()
    Connection.close()
    print('同乡查找成功')
    return id_group

def select_schoolfellow(id_selece=None, option_database=None):
    """
    :param id_selece: 需要查找校友关系的人物ID
    :param option_database: 数据库验证配置信息
    :return: 该人物的所有校友人物ID
    """
    Connection = Con_PostgreSQL(**option_database)
    select_sql = f"SELECT id_x, id_y FROM crawler.officer_schoolfellow WHERE id_x = '{id_selece}' OR id_y = '{id_selece}';"
    cur = Connection.cursor()
    cur.execute(select_sql)
    id_group = []
    for x in cur.fetchall():
        #print(result)
        if x[0] == id_selece and x[1] not in id_group:
            id_group.append(x[1])
        if x[1] == id_selece and x[0] not in id_group:
            id_group.append(x[0])
    cur.close()
    Connection.close()
    print('校友查找成功')
    return id_group

def select_message(id_selece=None, option_database=None):
    """
    :param id_selece: 需要查找所有信息的人物ID
    :param option_database: 数据库验证配置信息
    :return: 该人物的所有信息
    """
    Connection = Con_PostgreSQL(**option_database)
    select_sql = f"SELECT * FROM crawler.officer_message WHERE id_index = '{id_selece}';"
    cur = Connection.cursor()
    cur.execute(select_sql)
    print('信息查找成功')
    return cur.fetchall()

if __name__ == '__main__':
    select = select_schoolfellow('10612556', option_postgre)
    print(len(select), sorted(select))
