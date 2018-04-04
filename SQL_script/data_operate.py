from officer_process import Con_MySQL, Con_PostgreSQL, Con_Neo4j
import time

def data_migration(rel_type=None, relation=None, relationstr=None):
    """
    函数功能：将图数据库中的关系信息导入关系型数据库，关系代价值转化为亲密度类型。
    :param rel_type: 图数据库中的关系名称
    :param relation: 关系型数据库中的关系类型号
    :param relationstr: 关系型数据库中的关系名称
    :return: True
    """
    select_mysql = "SELECT * FROM demo.people_rel WHERE people_id='{0}' AND link_id='{1}' AND relation={2};"
    insert_mysql = "INSERT INTO demo.people_rel(people_id,link_id,link_name,intimate,relation,relationstr) VALUES ('{0}','{1}','{2}','{3}',{4},'{5}');"
    cur_mysql = mysql.cursor()
    # cur_postgre = postgre.cursor()
    max_value = float(graph.data(f"MATCH (:Person)-[r:{rel_type}]->(:Person) RETURN max(r.cost) as max;")[0]['max'])  # 最大代价值
    min_value = float(graph.data(f"MATCH (:Person)-[r:{rel_type}]->(:Person) RETURN min(r.cost) as min;")[0]['min'])  # 最小代价值
    results = graph.data(f"MATCH (a:Person)-[r:{rel_type}]-(b:Person) RETURN a,r,b;")
    i = count = 0  # 计数器
    for result in results:
        i += 1
        print(i, result['a']['name'], result['b']['name'])
        intimate = int(4*(float(result['r']['cost'])-min_value)/(max_value-min_value))+1  # 亲密度标准化
        cur_mysql.execute(select_mysql.format(result['a']['id'], result['b']['id'], relation))
        if not cur_mysql.fetchone():  # 如果记录不存在，则插入
            finish_insert_sql = insert_mysql.format(result['a']['id'], result['b']['id'], result['b']['name'], intimate, relation, relationstr)
            try:
                cur_mysql.execute(finish_insert_sql)
                mysql.commit()
                count += 1
                print(count, 'MySQL Insert Successful:', result['a']['id'], result['b']['id'], result['a']['name'], result['b']['name'], intimate, relation, relationstr)
            except:
                mysql.rollback()  # 插入失败，执行回滚操作
                print('\t\tMySQL Insert Error:', result['a']['id'], result['b']['id'], result['a']['name'], result['b']['name'], intimate, relation, relationstr)
    cur_mysql.close()
    # cur_postgre.close()
    return True

if __name__ == '__main__':
    time1 = time.clock()
    option_mysql = {'database': "demo", 'user': "root", 'password': "root", 'host': "opsrv.mapout.lan"}  # MySQL数据库连接配置
    option_postgre = {'database': "cof", 'user': "postgres", 'password': "postgres", 'host': "192.168.10.6"}  # PostgreSQL数据库连接验证
    option_neo4j = {'http': "http://opsrv.mapout.lan:7474", 'user': "neo4j", 'password': "Neo4j"}  # Neo4j图数据库连接配置
    mysql = Con_MySQL(**option_mysql)
    postgre = Con_PostgreSQL(**option_postgre)
    graph = Con_Neo4j(**option_neo4j)
    data_migration('workmate_with', 4, '同事')  # 导入同事关系
    data_migration('schoolfellow_with', 7, '同学')  # 导入同学关系
    mysql.close()
    postgre.close()
    time2 = time.clock()
    print('Execution Time:', time2 - time1, 's')
